"""An order must not be stranded between the processor's two transactions.

`OrderProcessor.handle` writes the claim and the order in one transaction (phase 1),
computes the score outside any transaction, then writes the decision in a second one
(phase 3). The event id is what joins them, and the module docstring claims the gap
is closed by it: "if the process dies in it, the event is redelivered and the handler
finds the order already in `accepted` and picks up from there."

It did not. `mark_processed` returning `False` short-circuited unconditionally, so a
redelivery after an interrupted phase 1 did nothing at all. The order stayed in
`accepted` forever: ledgered, present in the database, never scored, never approved,
never reviewed, and no number of redeliveries would change it. A permanently stuck
order with nothing to retry it is the worst outcome this pipeline can produce, and it
was the *normal* result of a consumer dying mid-handler.

The chaos test is what found it, and it could only be seen by reconciling by identity:
400 events published, 400 in the ledger, **399 orders**. A count check said "one
missing"; naming them said which.

The discriminator is the ledger's `effect` column: `accepted` means a delivery claimed
the event and did not finish, `scored` means it finished. A duplicate delivery of a
*finished* event must still do nothing at all -- that is invariant 6, and it is
asserted here too so the resume path cannot be "fixed" by removing it.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import func, select

from backend.database.models import FraudScore, Order, OrderStatus, ProcessedEvent
from backend.database.repositories import idempotency
from backend.events.schema import EventEnvelope, EventType, OrderAcceptedPayload, envelope_for
from backend.pipeline.order_processor import ORDER_PROCESSOR_GROUP, OrderProcessor


def _payload(order_id: str, *, customer_id: str = "CUST-1", total_cents: int = 4_800) -> Any:
    from backend.events.schema import LineItem, PaymentDetails

    return OrderAcceptedPayload(
        order_id=order_id,
        customer_id=customer_id,
        items=[LineItem(sku="SKU-TSHIRT-M", quantity=2, unit_price_cents=2_400)],
        payment=PaymentDetails(
            bin="411111",
            last4="4242",
            card_country="US",
            billing_country="US",
            shipping_country="US",
            ip_country="US",
        ),
        total_cents=total_cents,
        idempotency_key=f"resume-{order_id}",
    )


async def _envelope(order_id: str) -> EventEnvelope:
    return envelope_for(
        EventType.ORDER_ACCEPTED, _payload(order_id), order_id=order_id, correlation_id="corr-1"
    )


@pytest.fixture
def processor(
    settings: Any,
    metrics: Any,
    event_bus: Any,
    task_queue: Any,
    fraud_client: Any,
    session_factory: Any,
    seeded_warehouses: Any,  # noqa: ARG001 - seeds the catalogue the order reserves against
) -> OrderProcessor:
    return OrderProcessor(
        settings=settings,
        bus=event_bus,
        queue=task_queue,
        scorer=fraud_client,
        metrics=metrics,
        session_factory=session_factory,
    )


async def _counts(session_factory: Any, order_id: str) -> tuple[int, int]:
    async with session_factory() as session:
        orders = await session.execute(
            select(func.count()).select_from(Order).where(Order.id == order_id)
        )
        scores = await session.execute(
            select(func.count()).select_from(FraudScore).where(FraudScore.order_id == order_id)
        )
        return int(orders.scalar_one()), int(scores.scalar_one())


async def test_a_delivery_interrupted_after_phase_one_is_resumed(
    processor: OrderProcessor, session_factory: Any
) -> None:
    """The whole bug, end to end, with the interruption simulated.

    Phase 1 is made to fail *after* it has committed the claim and the order -- which
    is what a cancellation between the two writes looks like from the outside -- and
    the redelivery is then given the event that the real log would have redelivered.
    The order must reach a decided state.

    `MarkProcessedOnly` is the interruption: it lets phase 1's transaction commit and
    then raises, so the ledger row and the order row both exist with the claim still
    reading `accepted`.
    """
    order_id = "CRG-RESUME-1"
    envelope = await _envelope(order_id)

    # First delivery: phase 1 commits, then the handler dies.
    #
    # The interruption is injected at the scorer, which is called *after* phase 1's
    # transaction has committed and *before* phase 3's -- the exact window between
    # the two writes that this test is about.
    original_score = processor._score

    async def failing_score(_payload: Any) -> None:
        raise _Interrupted("the process died between the two writes")

    processor._score = failing_score  # type: ignore[method-assign, assignment]
    with pytest.raises(_Interrupted):
        await processor.handle(envelope)

    # The order and its ledger row both exist, and the claim is unfinished.
    orders, scores = await _counts(session_factory, order_id)
    assert orders == 1, "phase 1 should have written the order before the interruption"
    assert scores == 0, "phase 3 should not have run"

    async with session_factory() as session:
        assert await idempotency.claimed_effect(session, envelope.event_id) == "accepted"

    # Redelivery, as the log would do it.
    processor._score = original_score  # type: ignore[method-assign]
    await processor.handle(envelope)

    orders, scores = await _counts(session_factory, order_id)
    assert orders == 1, "the resume inserted a second order for the same event"
    assert scores == 1, "the resumed delivery never scored the order"

    async with session_factory() as session:
        assert await idempotency.claimed_effect(session, envelope.event_id) == "scored", (
            "the claim was never completed, so every future redelivery would resume "
            "again and duplicate its side effects"
        )
        status = (
            await session.execute(select(Order.status).where(Order.id == order_id))
        ).scalar_one()
        assert status == OrderStatus.SCORED, f"the order is still {status}"

    assert processor.stats.duplicates == 0, (
        "an interrupted claim was counted as a duplicate, which is the confusion this "
        "whole path is fixing"
    )


async def test_a_genuine_duplicate_still_does_nothing_at_all(
    processor: OrderProcessor, session_factory: Any
) -> None:
    """Invariant 6, asserted so the resume path cannot eat it.

    The resume fix adds a reason to keep going after a refused claim. This is the
    other reason to *not* go: a delivery that already finished must not re-apply its
    effects, re-publish a score, or enqueue a second review task. At-least-once
    delivery makes this the common case, not the rare one.
    """
    order_id = "CRG-DUPE-1"
    envelope = await _envelope(order_id)

    await processor.handle(envelope)
    first = await _counts(session_factory, order_id)
    assert first == (1, 1)

    await processor.handle(envelope)
    second = await _counts(session_factory, order_id)
    assert second == (1, 1), f"the duplicate delivery re-applied its effect: {second}"

    assert processor.stats.duplicates == 1
    assert processor.stats.handled == 1, "the duplicate was counted as freshly handled"


async def test_completion_is_written_in_the_phase_three_transaction(
    processor: OrderProcessor, session_factory: Any
) -> None:
    """A handled order's claim must read `scored` in the database.

    Pinned separately because the previous two tests would both still pass if the
    completion were committed somewhere else entirely, or written twice. This is the
    state a redelivery consults, so it is worth reading directly.
    """
    envelope = await _envelope("CRG-COMPLETE-1")
    await processor.handle(envelope)

    async with session_factory() as session:
        row = (
            await session.execute(
                select(ProcessedEvent).where(ProcessedEvent.event_id == envelope.event_id)
            )
        ).scalar_one()
        assert row.effect == "scored"
        assert row.topic == "order-events"
        assert row.consumer_group == ORDER_PROCESSOR_GROUP


class _Interrupted(RuntimeError):
    """Stand-in for a process dying mid-handler."""
