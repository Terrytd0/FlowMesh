"""The review worker: consume `review-queue` tasks, decide, act.

This is where a human enters the pipeline, and the shape of this worker is
determined by one fact: **the message can be delivered more than once, and the
human's decision must land exactly once.**

A task is acknowledged only after the decision and its inventory effect are both
committed. That ordering means:

- crash before the commit -> the message is redelivered -> `decide_review` sees a
  pending item and proceeds
- crash after the commit, before the ack -> the message is redelivered ->
  `decide_review` raises `AlreadyDecided` -> the worker acks and moves on

The second case is the one that produces "the customer was charged twice" in a
naive implementation, and `AlreadyDecided` is caught here and treated as success.
That is not swallowing an error; it is recognising the *expected* outcome of
at-least-once delivery. The decision it protects against -- acting on a decision
twice -- is prevented by the check constraint on `review_queue_items`, which the
database enforces even if this logic is wrong.

Notifications are an outbox, written in the same transaction as the decision.
The alternative -- send the email, then commit -- produces the failure this
avoids: an email that says "your order is approved" for an approval that rolled
back.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.config.settings import Settings
from backend.core.logging import get_logger
from backend.database.repositories import reviews as review_repo
from backend.events.bus import EventBus
from backend.events.schema import EventType, OrderDecisionPayload, ReviewStatus, Topic, envelope_for
from backend.observability.metrics import FlowMeshMetrics
from backend.queues.memory_queue import parse_task
from backend.queues.protocol import TaskQueue

logger = get_logger(__name__)

#: Consumer identity for the notification worker. Separate from the review group
#: because the two do different things and must be able to scale (and to fail)
#: independently.
NOTIFICATION_WORKER = "flowmesh-notification-worker-v1"


@dataclass
class WorkerStats:
    processed: int = 0
    already_decided: int = 0
    approved: int = 0
    rejected: int = 0
    failed: int = 0
    notifications: int = 0
    errors: list[str] = field(default_factory=list)


class ReviewWorker:
    """Handles review-queue tasks: applies a decision, or records a new request."""

    def __init__(
        self,
        *,
        settings: Settings,
        queue: TaskQueue,
        bus: EventBus,
        metrics: FlowMeshMetrics,
        session_factory: async_sessionmaker[AsyncSession],
        inventory_service_factory: Any = None,
        actor: str = "worker:review",
    ) -> None:
        self._settings = settings
        self._queue = queue
        self._bus = bus
        self._metrics = metrics
        self._sessions = session_factory
        self._inventory_factory = inventory_service_factory
        self._actor = actor
        self.stats = WorkerStats()

    # -- consuming ---------------------------------------------------------

    async def handle(self, body: bytes) -> None:
        """One task-queue message.

        Called by the transport's consumer, which acks only when this returns.
        Any raise propagates, so the message is nacked and redelivered.
        """
        try:
            task = parse_task(body)
        except ValueError as exc:
            # Unparseable payloads are poison messages. Raising forever would
            # block the queue behind them; the transport's retry limit
            # dead-letters this one, which is the correct terminal outcome.
            self.stats.failed += 1
            logger.error("unparseable review task: %s", exc)
            raise

        review_id = str(task.get("review_id", ""))
        order_id = str(task.get("order_id", ""))
        if not review_id or not order_id:
            self.stats.failed += 1
            logger.error("review task missing review_id or order_id: %s", task)
            raise ValueError("review task must carry review_id and order_id")

        decision = task.get("decision")
        if decision is None:
            # This is a *request* to review, not a decision. Make sure the row
            # exists and the reviewer can see it; the human's answer arrives via
            # the API, not via this queue.
            await self._ensure_visible(order_id=order_id, review_id=review_id, task=task)
            return

        await self._apply_decision(
            review_id=review_id,
            order_id=order_id,
            decision=decision,
            note=str(task.get("note", "")),
        )

    async def _ensure_visible(self, *, order_id: str, review_id: str, task: dict[str, Any]) -> None:
        """Confirm the review row is on the dashboard for this task.

        The row is normally written by the order processor. This exists for the
        case where the queue delivered but the transaction that should have
        written the row did not commit -- and an operator looking at an empty
        queue that a customer says is "waiting for review" needs the row to
        appear. Re-creating it is idempotent (`enqueue_review` returns the
        existing row).
        """
        from backend.events.schema import RiskBand

        async with self._sessions() as session, session.begin():
            band_text = str(task.get("band", "ambiguous"))
            try:
                band = RiskBand(band_text)
            except ValueError:
                band = RiskBand.AMBIGUOUS
            item, created = await review_repo.enqueue_review(
                session,
                order_id=order_id,
                score=float(task.get("score", 0.0)),
                band=band,
                reasons=list(task.get("reasons", [])),
                rationale=str(task.get("reason", "")),
                sla_seconds=self._settings.review_sla_seconds,
            )
            if created:
                logger.info(
                    "review row recreated from task order_id=%s review_id=%s", order_id, item.id
                )
            await review_repo.record_audit(
                session,
                order_id=order_id,
                entity="review",
                entity_id=item.id,
                action="visible",
                actor="worker:review",
                detail={"review_id": review_id, "reason": str(task.get("reason", ""))[:200]},
            )
        self.stats.processed += 1

    async def _apply_decision(
        self, *, review_id: str, order_id: str, decision: str, note: str
    ) -> None:
        """Record a decision and its inventory effect in one transaction."""
        target = ReviewStatus(decision) if decision in {s.value for s in ReviewStatus} else None
        if target is None:
            raise ValueError(f"unknown review decision {decision!r}")

        reservation: tuple[str, str] | None = None
        async with self._sessions() as session, session.begin():
            try:
                item = await review_repo.decide_review(
                    session, review_id=review_id, decision=target, actor=self._actor, note=note
                )
            except review_repo.AlreadyDecided as already:
                # The expected redelivery case. Acknowledge and move on.
                self.stats.already_decided += 1
                logger.info(
                    "review already decided review_id=%s status=%s (redelivery)",
                    already.item.id,
                    str(already.item.status),
                )
                return

            await review_repo.set_order_status_for_review(
                session, order_id=order_id, decision=target
            )

            if self._inventory_factory is not None:
                service = self._inventory_factory(session)
                warehouse_id = await _warehouse_for(session, order_id)
                if warehouse_id is not None:
                    if target == ReviewStatus.APPROVED:
                        committed = await service.commit_reservation(
                            order_id=order_id, warehouse_id=warehouse_id
                        )
                        action = "committed" if committed else "commit_noop"
                    else:
                        released = await service.release_reservation(
                            order_id=order_id, warehouse_id=warehouse_id, reason="review_rejected"
                        )
                        action = "released" if released else "release_noop"
                    reservation = (warehouse_id, action)
                    await review_repo.record_audit(
                        session,
                        order_id=order_id,
                        entity="inventory",
                        entity_id=order_id,
                        action=action,
                        actor=self._actor,
                        detail={"warehouse": warehouse_id},
                    )

            await review_repo.record_audit(
                session,
                order_id=order_id,
                entity="review",
                entity_id=item.id,
                action=f"decided_{target.value}",
                actor=self._actor,
                detail={"note": note[:200]},
            )
            await review_repo.enqueue_notification(
                session,
                order_id=order_id,
                channel="email",
                template=f"order-{target.value}",
                body=_notification_body(order_id, target),
            )

        self.stats.processed += 1
        if target == ReviewStatus.APPROVED:
            self.stats.approved += 1
        elif target == ReviewStatus.REJECTED:
            self.stats.rejected += 1
        self._metrics.queue_processed.labels(
            queue=self._settings.review_queue, outcome="approved"
        ).inc()

        # Publish the decision after the commit, so no consumer can observe it
        # before the state it describes exists.
        payload = OrderDecisionPayload(
            order_id=order_id,
            review_id=review_id,
            decided_by=self._actor,
            decision="approved" if target == ReviewStatus.APPROVED else "rejected",
            note=note[:500],
        )
        event = envelope_for(
            EventType.ORDER_APPROVED
            if target == ReviewStatus.APPROVED
            else EventType.ORDER_REJECTED,
            payload,
            order_id=order_id,
        )
        await self._bus.publish(Topic.ORDER, order_id, event)
        _ = reservation

    # -- notifications -----------------------------------------------------

    async def deliver_notifications(self, body: bytes) -> None:
        """Notification task handler: mark the outbox row as sent.

        "Sending" is writing `sent_at`. A real deployment would hand `body` to an
        email provider here; the part this project is actually demonstrating is
        that the notification exists in a transaction *before* anyone tries to
        send it, so a crash cannot produce a decision with no notification.
        """
        from backend.database.repositories import reviews as repo

        task = parse_task(body)
        async with self._sessions() as session, session.begin():
            pending = await repo.unsent_notifications(session, limit=int(task.get("limit", 10)))
            for notification in pending:
                await repo.mark_notification_sent(session, notification.id)
            self.stats.notifications += len(pending)
        self._metrics.notifications_sent.labels(reason="review_decision").inc(len(pending))


async def _warehouse_for(session: AsyncSession, order_id: str) -> str | None:
    """Which warehouse holds this order's stock, if any."""
    from sqlalchemy import select

    from backend.database.models import StockReservation

    result = await session.execute(
        select(StockReservation.warehouse_id).where(StockReservation.order_id == order_id).limit(1)
    )
    return result.scalar_one_or_none()


def _notification_body(order_id: str, status: ReviewStatus) -> str:
    if status == ReviewStatus.APPROVED:
        return f"Order {order_id} has been approved and is being prepared for dispatch."
    if status == ReviewStatus.REJECTED:
        return f"Order {order_id} was declined. No payment has been taken."
    return f"Order {order_id} is no longer under review."
