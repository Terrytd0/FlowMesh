"""Idempotency: the `processed_events` ledger.

One function carries the whole project's exactly-once-effect story, and its
placement is the design:

**`mark_processed` inserts on the primary key, inside the caller's transaction.**
It is not a `SELECT` that asks "have I seen this?" and it is not a flag set
after the write. Both of those have a window between the check and the effect,
and at 500 orders/sec with Kafka's at-least-once delivery, the window is hit
constantly rather than rarely.

With an `INSERT` on `event_id`, a concurrent duplicate delivery collides with
the constraint and one of them loses. There is no window, because the check and
the claim are the same statement.

The one subtlety, and it is a real one: the caller must have *not yet written
anything else* in this transaction when it calls this, or the `IntegrityError`
rollback discards that work. Every handler here claims first, then writes. The
ordering is documented at each call site because it is the sort of thing that
looks like an optimisation when someone reorders it.
"""

from __future__ import annotations

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.clock import utcnow
from backend.database.models import ProcessedEvent
from backend.database.repositories.inventory import rows_affected


async def mark_processed(
    session: AsyncSession,
    *,
    event_id: str,
    topic: str,
    group: str,
    order_id: str | None = None,
    effect: str = "",
    partition: int | None = None,
    log_offset: int | None = None,
) -> bool:
    """Claim an event. `True` if this caller is the one that got it.

    `False` means another delivery already applied it, and the caller must do
    nothing at all -- not retry, not re-apply, not log an error. A duplicate
    delivery is the normal case under at-least-once semantics, and a handler that
    treats it as an exception is a handler that stops consuming.

    The insert is wrapped in a SAVEPOINT (`begin_nested`), and that is load-bearing
    rather than defensive. Without it the `IntegrityError` is raised *inside the
    caller's transaction*, and SQLAlchemy's session is left in a state where every
    subsequent statement fails with `PendingRollbackError` -- not just this one.
    So the first version of this function looked correct: it caught the
    `IntegrityError` and returned `False`. What it actually did was convert a
    duplicate delivery from "silently ignored" into "consumer crashes on commit",
    which under at-least-once delivery means a poison message that is redelivered
    forever.

    The savepoint makes the rollback *local*: the constraint violation is undone and
    the outer transaction carries on, which is what "a duplicate is not an error"
    requires. `test_a_duplicate_claim_leaves_the_callers_transaction_usable` asserts
    the transaction survives, not just that the return value is `False` -- the
    return value alone would have passed with the broken version.
    """
    try:
        async with session.begin_nested():
            session.add(
                ProcessedEvent(
                    event_id=event_id,
                    topic=topic,
                    consumer_group=group,
                    order_id=order_id,
                    effect=effect,
                    partition=partition,
                    log_offset=log_offset,
                    applied_at=utcnow(),
                )
            )
            await session.flush()
    except IntegrityError:
        return False
    return True


async def was_processed(session: AsyncSession, event_id: str) -> bool:
    """Read-only lookup, for diagnostics and the reconciliation report.

    Explicitly not a substitute for `mark_processed`: a caller that checks this
    and then writes has reintroduced the race this module exists to remove.
    """
    result = await session.execute(
        select(func.count()).select_from(ProcessedEvent).where(ProcessedEvent.event_id == event_id)
    )
    return bool(result.scalar_one())


async def claimed_effect(session: AsyncSession, event_id: str) -> str | None:
    """The `effect` recorded against a claim, or `None` if there is no row.

    This exists because `mark_processed` returning `False` is genuinely ambiguous,
    and reading it as "already done" strands work:

    - a **completed** delivery, where doing nothing is correct and re-applying
      would double it;
    - a delivery that **claimed the event and then died** before its effect landed,
      where doing nothing loses the order permanently.

    Both look identical to the caller -- the insert collided either way. The
    `effect` column is what separates them, so a handler with a multi-transaction
    body can mark the claim in-progress on the way in and complete it on the way
    out, and a redelivery can tell "someone finished this" from "someone started
    it and died".
    """
    result = await session.execute(
        select(ProcessedEvent.effect).where(ProcessedEvent.event_id == event_id)
    )
    return result.scalar_one_or_none()


async def complete_claim(session: AsyncSession, *, event_id: str, effect: str) -> bool:
    """Mark a claim's effect as finished. `True` if this caller changed it.

    Call this in the *same transaction* as the effect it describes, so the claim
    cannot be left claiming work that was rolled back.

    The `effect != <in-progress>` predicate is what makes it idempotent: a
    redelivery that runs the completion again updates nothing and reports `False`,
    rather than rewriting a value that is already correct.
    """
    result = await session.execute(
        update(ProcessedEvent)
        .where(ProcessedEvent.event_id == event_id, ProcessedEvent.effect != effect)
        .values(effect=effect)
    )
    return rows_affected(result) > 0


async def processed_event_ids(
    session: AsyncSession, *, topic: str | None = None, group: str | None = None
) -> set[str]:
    """Every applied event id, for `scripts/chaos_test.py`'s reconciliation.

    Loads the ids rather than counting, because the question the chaos test asks
    is "which published events are missing from the ledger", and a count cannot
    answer it -- it can only tell you that some number is wrong.
    """
    statement = select(ProcessedEvent.event_id)
    if topic is not None:
        statement = statement.where(ProcessedEvent.topic == topic)
    if group is not None:
        statement = statement.where(ProcessedEvent.consumer_group == group)
    result = await session.execute(statement)
    return {row for row in result.scalars()}


async def effects_for_order(session: AsyncSession, order_id: str) -> list[str]:
    """What was applied for one order, in application order.

    Reads the audit trail's companion. Used by the chaos test to assert not just
    that an order was processed once, but that the *right* effect happened.
    """
    result = await session.execute(
        select(ProcessedEvent.effect)
        .where(ProcessedEvent.order_id == order_id)
        .order_by(ProcessedEvent.applied_at, ProcessedEvent.event_id)
    )
    return list(result.scalars())
