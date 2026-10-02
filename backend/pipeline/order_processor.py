"""The order processor: consume `order-events`, score, route.

This is the pipeline's centre of gravity and the module that has to be right
about three things at once -- idempotency, ordering, and what happens when a
service is down.

**One event in, one decision out, exactly once in effect.** The handler claims
the event in `processed_events` *first*, in the same transaction as every write
that follows, and returns only when all of it is committed. If it crashes
halfway, the transaction rolls back and the uncommitted offset means the event
comes back. There is no path where the offset advances but the work did not
happen, and no path where the work happened twice.

**The gRPC call is outside the database transaction.** This is not a style
choice. On PostgreSQL a row lock lives until commit, so a scoring call -- 2ms
in-process, 200ms over a real network -- held inside the transaction would
serialise every concurrent order touching the same rows behind it. The order is
therefore written first, the score computed outside, and the result written
second. The gap between the two writes is closed by the event id: if the process
dies in it, the event is redelivered and the handler finds the order already in
`accepted` and picks up from there.

**Scoring failure holds the order rather than dropping it.** An order whose score
could not be obtained is *held*, with the reason recorded, and the event is
committed. Dropping it would lose a customer's order; retrying forever would stall
the partition and stop every order behind it. Holding puts it on a dashboard
where a human can see it, which is the only outcome that is both safe and
visible.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.config.settings import Settings
from backend.core.clock import monotonic
from backend.core.logging import get_logger
from backend.database.repositories import fraud as fraud_repo
from backend.database.repositories import idempotency
from backend.database.repositories import orders as order_repo
from backend.database.repositories import reviews as review_repo
from backend.events.bus import EventBus, Subscription
from backend.events.schema import (
    EventEnvelope,
    EventType,
    OrderAcceptedPayload,
    OrderScoredPayload,
    OrderStatus,
    Topic,
)
from backend.fraud.engine import FraudDecision
from backend.fraud.features import OrderContext
from backend.grpc_service.client import FraudScoringBackend, FraudScoringError
from backend.observability.metrics import FlowMeshMetrics
from backend.pipeline.routing import APPROVE, route_for
from backend.queues.protocol import TaskQueue

logger = get_logger(__name__)

#: Consumer group for this stage. A name change is a redelivery of the entire
#: topic, which is why it is a constant here rather than a config value: it is
#: not a knob, it is an identity.
ORDER_PROCESSOR_GROUP = "flowmesh-order-processor-v1"

#: How many orders are scored concurrently. Concurrency here buys throughput
#: because the scoring call is I/O-bound; it is bounded because unbounded
#: concurrency against one gRPC server just moves the queue into the server and
#: converts a fast failure into a slow one.
DEFAULT_SCORING_CONCURRENCY = 32

#: How many shortfall order ids are kept for the load report. A sample, not a log:
#: at 500 orders/sec a list of every shortfall would be an unbounded growth path in
#: a long-running process, and the count is the number anyone actually reads.
_SHORTFALL_SAMPLE_LIMIT = 50

#: The ledger `effect` values that distinguish a started claim from a finished one.
#:
#: `mark_processed` returning `False` cannot tell "a previous delivery finished this"
#: from "a previous delivery died mid-handler", and treating both as finished is how
#: an order ends up permanently stranded -- see `handle`. These two values are the
#: discriminator, and `_CLAIM_IN_PROGRESS` is deliberately a value no finished
#: delivery ever leaves behind.
_CLAIM_IN_PROGRESS = "accepted"
_CLAIM_APPLIED = "scored"


@dataclass
class ProcessorStats:
    """Counters the consumer logs and the smoke script asserts on."""

    handled: int = 0
    duplicates: int = 0
    held_on_scoring_error: int = 0
    approved: int = 0
    held: int = 0


class OrderProcessor:
    """Consumes order events and drives each order to approval or review."""

    def __init__(
        self,
        *,
        settings: Settings,
        bus: EventBus,
        queue: TaskQueue,
        scorer: FraudScoringBackend,
        metrics: FlowMeshMetrics,
        session_factory: async_sessionmaker[AsyncSession],
        inventory_service_factory: Any = None,
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._queue = queue
        self._scorer = scorer
        self._metrics = metrics
        self._sessions = session_factory
        self._inventory_factory = inventory_service_factory
        self.stats = ProcessorStats()
        self._semaphore = asyncio.Semaphore(DEFAULT_SCORING_CONCURRENCY)
        #: Latch for the "ignoring <type>" line, so a load run does not produce one
        #: per order. See `handle`.
        self._logged_other_types = False
        #: Order ids that were auto-approved on score but could not be reserved,
        #: and so went to review as a *fulfilment* problem rather than a fraud one.
        #: Bounded, because at a sustained rate this list would grow without limit
        #: in a process intended to run for weeks; the count is what the load report
        #: uses, and the sample is what an operator looks at.
        self.shortfalls: list[str] = []

    # -- the handler -------------------------------------------------------

    async def handle(self, envelope: EventEnvelope) -> None:
        """One `order.accepted` event.

        Everything downstream of the claim is in one transaction. The scoring
        call is the sole exception and is made between two short transactions --
        see the module docstring for why.
        """
        if envelope.event_type != EventType.ORDER_ACCEPTED:
            if not self._logged_other_types:
                # `order.scored` is this stage's own output, arriving back around
                # the log. Logging a line per occurrence is 500 lines/sec at the
                # target rate, which buries the redelivery and error lines that
                # actually need reading. Logged once per process, then silent.
                #
                # The offset still commits either way: a message this consumer
                # cannot act on is not a message it should retry, and a future
                # event type is handled by whichever version of this consumer knows
                # about it.
                self._logged_other_types = True
                logger.info(
                    "ignoring %s on the order topic; this consumer handles %s only",
                    str(envelope.event_type),
                    str(EventType.ORDER_ACCEPTED),
                )
            return

        payload = OrderAcceptedPayload.model_validate(envelope.payload)
        order_id = payload.order_id

        # Phase 1: claim the event and persist the order.
        resuming = False
        async with self._sessions() as session, session.begin():
            claimed = await idempotency.mark_processed(
                session,
                event_id=envelope.event_id,
                topic=str(Topic.ORDER),
                group=ORDER_PROCESSOR_GROUP,
                order_id=order_id,
                effect=_CLAIM_IN_PROGRESS,
                partition=envelope.partition,
                log_offset=envelope.offset,
            )
            if not claimed:
                # `False` is ambiguous, and reading it as "already finished" loses
                # orders. The event id is the join between the two writes of this
                # handler: phase 1 commits the claim *and* the order row, then the
                # score is computed outside any transaction, then phase 3 applies
                # it. A delivery that dies in that gap has left a claim at
                # `_CLAIM_IN_PROGRESS` with no score behind it -- and the
                # redelivery used to return here, unconditionally, so the order sat
                # in `accepted` forever: ledgered, present, never scored, never
                # approved, never reviewed, and no amount of redelivery would
                # change it.
                #
                # The chaos test is what found it: 400 events published, 400 in the
                # ledger, 399 orders, because exactly one order was claimed by a
                # delivery that was cancelled between the two writes. The count
                # difference is one row; the bug is a permanently stranded order
                # with nothing to retry it.
                #
                # So a claim still marked in-progress means "a previous delivery
                # started this and did not finish", and the handler resumes at
                # phase 2. A claim that has been completed means a genuine
                # duplicate, and the handler still does nothing at all.
                in_progress = (
                    await idempotency.claimed_effect(session, envelope.event_id)
                    == _CLAIM_IN_PROGRESS
                )
                if not in_progress:
                    self.stats.duplicates += 1
                    self._metrics.events_deduplicated.labels(topic=str(Topic.ORDER)).inc()
                    logger.info("duplicate order event skipped event_id=%s", envelope.event_id)
                    return
                resuming = True
                logger.info(
                    "resuming an interrupted order event event_id=%s order_id=%s",
                    envelope.event_id,
                    order_id,
                )

            if not resuming:
                existing = await order_repo.get_order(session, order_id)
                if existing is None:
                    await order_repo.insert_order(
                        session,
                        order_id=order_id,
                        customer_id=payload.customer_id,
                        total_cents=payload.total_cents,
                        correlation_id=envelope.correlation_id,
                        idempotency_key=payload.idempotency_key,
                        items=[item.model_dump() for item in payload.items],
                    )
                await review_repo.record_audit(
                    session,
                    order_id=order_id,
                    entity="order",
                    entity_id=order_id,
                    action="accepted",
                    actor="pipeline:order-processor",
                    detail={"event_id": envelope.event_id, "items": len(payload.items)},
                    correlation_id=envelope.correlation_id,
                )

        # Phase 2: score, outside any transaction.
        decision = await self._score(payload)

        # Phase 3: apply the decision.
        await self._apply_decision(envelope=envelope, payload=payload, decision=decision)

    async def _score(self, payload: OrderAcceptedPayload) -> FraudDecision | None:
        """Call the scorer, with a bounded concurrency gate.

        Returns `None` when scoring failed. `None` is not the same as a low score
        and the caller treats it differently: a low score approves, an unknown
        score holds.
        """
        context = OrderContext(
            order_id=payload.order_id,
            customer_id=payload.customer_id,
            total_cents=payload.total_cents,
            items=tuple((item.sku, item.quantity) for item in payload.items),
            card_bin=payload.payment.bin,
            card_country=payload.payment.card_country,
            billing_country=payload.payment.billing_country,
            shipping_country=payload.payment.shipping_country,
            ip_country=payload.payment.ip_country,
            coupon_code=payload.payment.coupon_code,
            is_gift_card=payload.payment.is_gift_card,
            received_at=payload.received_at,
        )
        async with self._semaphore:
            started = monotonic()
            try:
                return await self._scorer.score(context, require_rationale=False)
            except FraudScoringError as exc:
                self._metrics.scoring_errors.labels(reason="scorer_unavailable").inc()
                logger.error(
                    "scoring failed order_id=%s after %.1fms: %s",
                    payload.order_id,
                    (monotonic() - started) * 1000,
                    exc,
                )
                return None

    async def _apply_decision(
        self,
        *,
        envelope: EventEnvelope,
        payload: OrderAcceptedPayload,
        decision: FraudDecision | None,
    ) -> None:
        """Write the score, move the order, and route it."""
        if decision is None:
            # Scoring failure holds the order. Deliberately commits this event:
            # retrying forever would stall the partition and every order behind
            # it, and the held order is now visible on a dashboard.
            async with self._sessions() as session, session.begin():
                await order_repo.set_order_status(
                    session,
                    order_id=payload.order_id,
                    status=OrderStatus.SCORED,
                    hold_reason="fraud scoring unavailable; held for manual review",
                )
                await review_repo.record_audit(
                    session,
                    order_id=payload.order_id,
                    entity="order",
                    entity_id=payload.order_id,
                    action="held_scoring_unavailable",
                    actor="pipeline:order-processor",
                    detail={"error": "scorer unavailable"},
                    correlation_id=envelope.correlation_id,
                )
                # A held order is a *decided* order, so the claim is closed even
                # though no score was produced. Leaving it `_CLAIM_IN_PROGRESS`
                # would make every redelivery of this event resume and re-hold it,
                # which under at-least-once delivery means an unbounded trail of
                # duplicate review work for an order that was already routed to a
                # human.
                await idempotency.complete_claim(
                    session, event_id=envelope.event_id, effect=_CLAIM_APPLIED
                )
            self.stats.held_on_scoring_error += 1
            self.stats.held += 1
            return

        route = route_for(
            score=decision.score,
            band=decision.band,
            degraded=decision.degraded,
            low_threshold=self._settings.fraud_low_threshold,
            high_threshold=self._settings.fraud_high_threshold,
        )

        async with self._sessions() as session, session.begin():
            recorded = await fraud_repo.insert_score(
                session,
                event_id=envelope.event_id,
                order_id=payload.order_id,
                score=decision.score,
                band=decision.band,
                model_version=decision.model_version,
                reasons=decision.reasons,
                rationale=decision.rationale,
                contributions={
                    "features": decision.features,
                    "top": [
                        {
                            "feature": contribution.feature,
                            "value": contribution.value,
                            "weight": contribution.weight,
                            "contribution": contribution.contribution,
                        }
                        for contribution in decision.contributions[:5]
                    ],
                },
                degraded=decision.degraded,
                llm_used=decision.llm_used,
                transport=self._scorer.transport,
                latency_ms=decision.latency_ms,
            )
            if not recorded:
                self.stats.duplicates += 1
                self._metrics.events_deduplicated.labels(topic=str(Topic.ORDER)).inc()
                return

            # Close the claim, in the same transaction as the effect it describes.
            #
            # This is what tells a later redelivery that the decision was applied.
            # Without it the ledger still reads `_CLAIM_IN_PROGRESS` forever, every
            # redelivery of the event resumes instead of no-ops, and the handler
            # re-runs its side effects (a second `order.scored`, a second review
            # task) on every at-least-once delivery.
            #
            # In this transaction rather than after it, so the claim cannot claim
            # work that rolled back.
            await idempotency.complete_claim(
                session, event_id=envelope.event_id, effect=_CLAIM_APPLIED
            )

            await order_repo.set_order_status(
                session,
                order_id=payload.order_id,
                status=OrderStatus.SCORED,
                hold_reason=route.reason if route.needs_review else None,
                fraud_score=decision.score,
                fraud_band=decision.band,
            )
            await review_repo.record_audit(
                session,
                order_id=payload.order_id,
                entity="order",
                entity_id=payload.order_id,
                action="scored",
                actor="pipeline:order-processor",
                detail={
                    "score": round(decision.score, 4),
                    "band": str(decision.band),
                    "route": route.action,
                    "reasons": decision.reasons,
                    "degraded": decision.degraded,
                },
                correlation_id=envelope.correlation_id,
            )

        self.stats.handled += 1

        # Publish the decision *after* the transaction commits, for the reason in
        # the module docstring: an event must never be visible before the state it
        # describes.
        await self._publish_scored(envelope, payload, decision)

        if route.action == APPROVE:
            await self._approve(envelope=envelope, payload=payload, decision=decision)
        else:
            await self._hold(
                envelope=envelope, payload=payload, decision=decision, reason=route.reason
            )

    # -- routing -----------------------------------------------------------

    async def _publish_scored(
        self, envelope: EventEnvelope, payload: OrderAcceptedPayload, decision: FraudDecision
    ) -> None:
        scored = OrderScoredPayload(
            order_id=payload.order_id,
            customer_id=payload.customer_id,
            score=decision.score,
            band=decision.band,
            reasons=decision.reasons,
            rationale=decision.rationale,
            model_version=decision.model_version,
            latency_ms=decision.latency_ms,
            transport=self._scorer.transport,
            degraded=decision.degraded,
        )
        event = envelope_for_scored(scored, correlation_id=envelope.correlation_id)
        await self._bus.publish(Topic.ORDER, payload.order_id, event)

    async def _approve(
        self, *, envelope: EventEnvelope, payload: OrderAcceptedPayload, decision: FraudDecision
    ) -> None:
        """Low score: reserve stock and mark the order approved."""
        self.stats.approved += 1
        lines = [{"sku": item.sku, "quantity": item.quantity} for item in payload.items]
        outcome = None
        if self._inventory_factory is not None:
            async with self._sessions() as session, session.begin():
                service = self._inventory_factory(session)
                outcome = await service.reserve_for_order(order_id=payload.order_id, lines=lines)
                await review_repo.record_audit(
                    session,
                    order_id=payload.order_id,
                    entity="inventory",
                    entity_id=payload.order_id,
                    action="reserved" if outcome.reserved else "shortfall",
                    actor="pipeline:order-processor",
                    detail={
                        "warehouse": outcome.warehouse_id,
                        "reservation": outcome.reservation_id,
                        "reason": outcome.reason,
                    },
                    correlation_id=envelope.correlation_id,
                )
                if outcome.reserved:
                    await order_repo.set_order_status(
                        session, order_id=payload.order_id, status=OrderStatus.RESERVED
                    )
                else:
                    await order_repo.set_order_status(
                        session,
                        order_id=payload.order_id,
                        status=OrderStatus.SCORED,
                        hold_reason=f"stock unavailable: {outcome.reason}",
                    )

        if outcome is not None and not outcome.reserved:
            # Stock is missing. The order joins the review queue as a *fulfilment*
            # problem rather than a fraud one -- same queue, different reason, and
            # a reviewer needs to be able to tell them apart, which is why the
            # reason string is the one the dashboard groups on.
            if len(self.shortfalls) < _SHORTFALL_SAMPLE_LIMIT:
                self.shortfalls.append(payload.order_id)
            await self._hold(
                envelope=envelope,
                payload=payload,
                decision=decision,
                reason=f"stock unavailable: {outcome.reason}",
                rationale=False,
            )

    async def _hold(
        self,
        *,
        envelope: EventEnvelope,
        payload: OrderAcceptedPayload,
        decision: FraudDecision,
        reason: str,
        rationale: bool = False,
    ) -> None:
        """High, ambiguous, degraded, or unservable: publish a review request."""
        self.stats.held += 1
        async with self._sessions() as session, session.begin():
            item, created = await review_repo.enqueue_review(
                session,
                order_id=payload.order_id,
                score=decision.score,
                band=decision.band,
                reasons=decision.reasons,
                rationale=decision.rationale,
                sla_seconds=self._settings.review_sla_seconds,
            )
            await review_repo.record_audit(
                session,
                order_id=payload.order_id,
                entity="review",
                entity_id=item.id,
                action="queued" if created else "queued_duplicate",
                actor="pipeline:order-processor",
                detail={"reason": reason, "score": round(decision.score, 4)},
                correlation_id=envelope.correlation_id,
            )

        # Published to the task queue, not the event log. That is the whole point
        # of running two brokers -- see ADR-004.
        from backend.queues.memory_queue import json_task

        task = json_task(
            {
                "review_id": item.id,
                "order_id": payload.order_id,
                "score": decision.score,
                "band": str(decision.band),
                "reasons": decision.reasons,
                "reason": reason,
                "correlation_id": envelope.correlation_id,
                "needs_rationale": rationale,
            }
        )
        await self._queue.publish(
            self._settings.review_queue, task, headers={"x-order-id": payload.order_id}
        )

    # -- wiring ------------------------------------------------------------

    def subscription(self, instances: int = 1, instance_id: int = 0) -> Subscription:
        """The `EventBus` subscription this processor runs."""
        return Subscription(
            topic=Topic.ORDER,
            group=ORDER_PROCESSOR_GROUP,
            handler=self.handle,
            instances=instances,
            instance_id=instance_id,
        )

    async def start(self, instances: int = 1, instance_id: int = 0) -> Any:
        return await self._bus.subscribe(self.subscription(instances, instance_id))

    async def stop(self, handle: Any) -> None:
        await self._bus.cancel(handle)


def envelope_for_scored(payload: OrderScoredPayload, *, correlation_id: str) -> EventEnvelope:
    """Build the `order.scored` envelope, in one place so the shape cannot drift."""
    from backend.events.schema import envelope_for

    return envelope_for(EventType.ORDER_SCORED, payload, correlation_id=correlation_id)
