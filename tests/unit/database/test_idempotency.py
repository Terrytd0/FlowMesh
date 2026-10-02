"""The idempotency ledger, and specifically what it does on a duplicate.

Every test here is about the `False` branch. The `True` branch is one insert and
proves nothing; the interesting property is that losing the race is *not* an error,
and the version of `mark_processed` that caught `IntegrityError` without a savepoint
passed a return-value test while turning every duplicate delivery into a
`PendingRollbackError` at commit time. That is the shape of bug this file exists to
prevent, so the assertions are about transaction state, not about return values.

`concurrent_factory` rather than the shared `session` fixture: see its docstring.
These tests need two independent connections to have a duplicate at all, and
`sqlite:///:memory:` with `StaticPool` gives every session the same one.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.database.repositories import idempotency as idem


async def test_the_first_claim_wins(session: AsyncSession) -> None:
    claimed = await idem.mark_processed(
        session, event_id="evt-1", topic="order-events", group="order-processor"
    )
    assert claimed is True
    assert await idem.was_processed(session, "evt-1") is True


async def test_a_duplicate_claim_is_refused(session: AsyncSession) -> None:
    await idem.mark_processed(
        session, event_id="evt-1", topic="order-events", group="order-processor"
    )
    assert (
        await idem.mark_processed(
            session, event_id="evt-1", topic="order-events", group="order-processor"
        )
        is False
    )


async def test_a_duplicate_claim_leaves_the_callers_transaction_usable(
    session: AsyncSession,
) -> None:
    """The test that catches the broken version.

    `mark_processed` used to `session.add(...)` outside any savepoint and catch the
    `IntegrityError`. The exception was handled, the function returned `False`, and
    the next `commit()` raised `PendingRollbackError` -- because SQLAlchemy marks the
    *whole* transaction as needing rollback once a statement fails. A duplicate
    delivery is the normal case under at-least-once delivery, so that turned the
    most common event in the system into a crash on the least common path.

    So this asserts the transaction still works *after* the refusal: a further claim
    for a different event, a write, and a commit.
    """
    await idem.mark_processed(
        session, event_id="evt-1", topic="order-events", group="order-processor"
    )
    refused = await idem.mark_processed(
        session, event_id="evt-1", topic="order-events", group="order-processor"
    )
    assert refused is False

    # Same session, same transaction, after the constraint violation.
    assert (
        await idem.mark_processed(
            session, event_id="evt-2", topic="order-events", group="order-processor"
        )
        is True
    )
    await session.commit()

    assert await idem.was_processed(session, "evt-2") is True


async def test_a_duplicate_is_refused_across_connections(
    concurrent_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The real race: two consumers, one event, two transactions.

    The single-session test above can only produce a duplicate because it wrote the
    row itself first. This one has the loser *roll back and re-try* the same claim,
    which is what actually happens when a Kafka redelivery races a committed offset.
    """
    async with concurrent_factory() as first:
        await first.begin()
        assert (
            await idem.mark_processed(
                first, event_id="evt-race", topic="order-events", group="order-processor"
            )
            is True
        )
        await first.commit()

    async with concurrent_factory() as second:
        await second.begin()
        assert (
            await idem.mark_processed(
                second, event_id="evt-race", topic="order-events", group="order-processor"
            )
            is False
        )
        # The point of the test: committing a refused claim is not an error.
        await second.commit()

        assert await idem.was_processed(second, "evt-race") is True


async def test_a_duplicate_delivery_records_exactly_one_effect(
    concurrent_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Refusing twice must not insert twice either.

    Cheap to assert and it is the failure mode of an "optimisation" that catches the
    error and then adds the object again: the row count is the only thing that
    notices.
    """
    for _ in range(3):
        async with concurrent_factory() as attempt:
            await attempt.begin()
            await idem.mark_processed(
                attempt,
                event_id="evt-thrice",
                topic="order-events",
                group="order-processor",
                order_id="ORD-1",
                effect="order.accepted",
            )
            await attempt.commit()

    async with concurrent_factory() as check:
        ids = await idem.processed_event_ids(check, topic="order-events")
        effects = await idem.effects_for_order(check, "ORD-1")

    assert ids == {"evt-thrice"}
    assert effects == ["order.accepted"]


async def test_the_ledger_filters_by_topic_and_group(session: AsyncSession) -> None:
    """Reconciliation compares ids across groups, so the filters have to work.

    Two groups applying the same event id is not a duplicate -- they are two
    different subscriptions to one log, and each owes its own effect. `event_id` is
    the primary key, so this also pins down that the key is not
    (event_id, consumer_group) as it would be if idempotency were per-subscriber.
    """
    await idem.mark_processed(
        session, event_id="evt-shared", topic="order-events", group="order-processor"
    )
    assert (
        await idem.mark_processed(
            session, event_id="evt-shared", topic="order-events", group="inventory-worker"
        )
        is False
    )

    assert await idem.processed_event_ids(session, group="order-processor") == {"evt-shared"}
    assert await idem.processed_event_ids(session, group="inventory-worker") == set()
    assert await idem.processed_event_ids(session, topic="inventory-events") == set()


@pytest.mark.parametrize("field", ["partition", "log_offset"])
async def test_delivery_coordinates_are_recorded(session: AsyncSession, field: str) -> None:
    """Offsets are kept because the chaos report reconciles against them.

    Not a status field: a log offset recorded against an applied event is what lets
    the replay report say "these events were re-delivered from offset N" rather than
    only "N events were seen twice".
    """
    coordinates: dict[str, Any] = {field: 41}
    await idem.mark_processed(
        session,
        event_id="evt-coords",
        topic="order-events",
        group="order-processor",
        **coordinates,
    )
    await session.commit()

    from sqlalchemy import select

    from backend.database.models import ProcessedEvent

    with session.no_autoflush:
        stored = (
            await session.execute(
                select(ProcessedEvent).where(ProcessedEvent.event_id == "evt-coords")
            )
        ).scalar_one()

    assert getattr(stored, field) == 41


# ------------------------------------------------- started vs finished


async def test_the_claim_records_that_it_is_still_in_progress(session: AsyncSession) -> None:
    """A fresh claim reads back as in-progress, not as finished.

    `mark_processed` returning `False` is ambiguous: it means either "a previous
    delivery finished this" or "a previous delivery claimed it and then died". Before
    the `effect` column was used as the discriminator, both were read as finished.
    """
    await idem.mark_processed(
        session,
        event_id="evt-inflight",
        topic="order-events",
        group="order-processor",
        effect="accepted",
    )
    await session.commit()

    assert await idem.claimed_effect(session, "evt-inflight") == "accepted"
    assert await idem.claimed_effect(session, "evt-never-seen") is None


async def test_completing_a_claim_changes_it_once(session: AsyncSession) -> None:
    """Completion is idempotent, and reports whether it was the one that did it.

    The `effect != <new>` predicate is what makes a redelivery a no-op. Without it a
    second completion would rewrite a value that is already right and report success,
    and the caller could no longer tell "I finished this" from "this was already
    finished" -- which is the distinction the resume path turns on.
    """
    await idem.mark_processed(
        session,
        event_id="evt-complete",
        topic="order-events",
        group="order-processor",
        effect="accepted",
    )
    await session.commit()

    assert await idem.complete_claim(session, event_id="evt-complete", effect="scored") is True
    await session.commit()
    assert await idem.claimed_effect(session, "evt-complete") == "scored"

    assert await idem.complete_claim(session, event_id="evt-complete", effect="scored") is False
    await session.commit()


async def test_completing_a_claim_that_does_not_exist_changes_nothing(
    session: AsyncSession,
) -> None:
    """It must not invent a row, and it must not raise.

    The claim is written by `mark_processed` in the same transaction as the effect, so
    a completion for an unknown event id means the caller has lost track. Reporting
    `False` -- "I changed nothing" -- is the honest answer; raising here would turn a
    bookkeeping mistake into a consumer that stops.
    """
    assert await idem.complete_claim(session, event_id="evt-absent", effect="scored") is False
    assert await idem.processed_event_ids(session) == set()


async def test_completion_survives_a_rollback_with_its_effect(session: AsyncSession) -> None:
    """Rolling back the effect rolls back the completion with it.

    The completion has to be in the *same transaction* as the effect it describes.
    Committed separately, a crash between the two would leave a claim reading
    "finished" for work that was rolled back -- and the redelivery would then skip it
    and lose it, which is the one outcome the ledger exists to prevent.
    """
    await idem.mark_processed(
        session,
        event_id="evt-rollback",
        topic="order-events",
        group="order-processor",
        effect="accepted",
    )
    await session.commit()

    await idem.complete_claim(session, event_id="evt-rollback", effect="scored")
    await session.rollback()

    assert await idem.claimed_effect(session, "evt-rollback") == "accepted", (
        "the completion outlived the rollback of the effect it described"
    )
