"""Review queue and audit persistence.

The review queue is where a human decision lands, so this module cares about two
things a normal CRUD layer would not:

- **A decided item always has an actor and a timestamp.** Enforced by a check
  constraint on the table *and* re-asserted here, because an approval nobody can
  attribute is indistinguishable from an approval nobody made.
- **Deciding is idempotent.** A double-clicked approve button, or a redelivered
  review task, must not flip a rejected order to approved. `decide_review` checks
  the current status and returns the existing decision rather than overwriting.

Both are the same lesson from a different angle: this is the only part of the
pipeline where a human is in the loop, so it is the only part where "the system
did something twice" is immediately visible to a person.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.clock import utcnow
from backend.core.ids import new_review_id
from backend.database.models import AuditLog, Notification, ReviewQueueItem
from backend.database.repositories.inventory import rows_affected as _rows_affected
from backend.events.schema import OrderStatus, ReservationState, ReviewStatus, RiskBand


class AlreadyDecided(Exception):
    """The item is no longer pending, and the existing decision is attached."""

    def __init__(self, item: ReviewQueueItem) -> None:
        super().__init__(f"review {item.id} is already {item.status}")
        self.item = item


# --------------------------------------------------------------------------
# Review queue
# --------------------------------------------------------------------------


async def enqueue_review(
    session: AsyncSession,
    *,
    order_id: str,
    score: float,
    band: RiskBand,
    reasons: list[str],
    rationale: str,
    sla_seconds: int,
    task_id: str | None = None,
) -> tuple[ReviewQueueItem, bool]:
    """Put an order in the review queue. `(item, created)`.

    `created=False` means it was already there, which is the normal outcome of a
    redelivered `review.requested`. The caller still publishes the task; the
    review worker is what makes that safe, and this makes the database agree.
    """
    existing = await get_review(session, order_id=order_id)
    if existing is not None:
        return existing, False
    item = ReviewQueueItem(
        id=new_review_id(),
        order_id=order_id,
        status=ReviewStatus.PENDING,
        score=score,
        band=band,
        reasons=list(reasons),
        rationale=rationale,
        sla_deadline=utcnow() + timedelta(seconds=sla_seconds),
        task_id=task_id,
    )
    session.add(item)
    try:
        await session.flush()
    except Exception:
        # Concurrent enqueue won. Re-read and report `created=False`.
        await session.rollback()
        found = await get_review(session, order_id=order_id)
        if found is None:
            raise
        return found, False
    return item, True


async def get_review(session: AsyncSession, *, order_id: str) -> ReviewQueueItem | None:
    result = await session.execute(
        select(ReviewQueueItem).where(ReviewQueueItem.order_id == order_id)
    )
    return result.scalar_one_or_none()


async def get_review_by_id(session: AsyncSession, review_id: str) -> ReviewQueueItem | None:
    result = await session.execute(select(ReviewQueueItem).where(ReviewQueueItem.id == review_id))
    return result.scalar_one_or_none()


async def pending_reviews(
    session: AsyncSession, *, limit: int = 50, oldest_first: bool = True
) -> list[ReviewQueueItem]:
    """Pending items, defaulting to oldest-first.

    Oldest-first is the reviewer's actual priority -- an item that has been
    waiting an hour costs more than one that arrived a second ago -- and it is
    also the SLA order, so the top of this list is the list that is about to
    breach.
    """
    direction = (
        ReviewQueueItem.sla_deadline.asc() if oldest_first else ReviewQueueItem.sla_deadline.desc()
    )
    result = await session.execute(
        select(ReviewQueueItem)
        .where(ReviewQueueItem.status == ReviewStatus.PENDING)
        .order_by(direction)
        .limit(limit)
    )
    return list(result.scalars())


async def breached_reviews(
    session: AsyncSession, *, now: datetime | None = None
) -> list[ReviewQueueItem]:
    """Pending items past their SLA deadline."""
    moment = now or utcnow()
    result = await session.execute(
        select(ReviewQueueItem).where(
            ReviewQueueItem.status == ReviewStatus.PENDING,
            ReviewQueueItem.sla_deadline < moment,
        )
    )
    return list(result.scalars())


async def decide_review(
    session: AsyncSession,
    *,
    review_id: str,
    decision: ReviewStatus,
    actor: str,
    note: str = "",
) -> ReviewQueueItem:
    """Record a human decision.

    Raises `AlreadyDecided` if the item is not pending. Refusing is the correct
    behaviour, not an error to swallow: an approval arriving after the reservation
    TTL has already released the stock is a decision about nothing, and reporting
    success would leave the caller believing the order is going out.
    """
    item = await get_review_by_id(session, review_id)
    if item is None:
        raise LookupError(f"no review item {review_id!r}")
    if item.status != ReviewStatus.PENDING:
        raise AlreadyDecided(item)

    statement = (
        update(ReviewQueueItem)
        .where(ReviewQueueItem.id == review_id, ReviewQueueItem.status == ReviewStatus.PENDING)
        .values(
            status=decision,
            decided_by=actor,
            decided_at=utcnow(),
            decision_note=note or None,
            updated_at=utcnow(),
        )
    )
    result = await session.execute(statement)
    if not _rows_affected(result):
        # Lost the race between the read and the write. Re-read and report.
        refreshed = await get_review_by_id(session, review_id)
        if refreshed is None:
            raise LookupError(f"no review item {review_id!r}")
        raise AlreadyDecided(refreshed)
    await session.flush()
    return item


async def review_stats(session: AsyncSession, *, now: datetime | None = None) -> dict[str, Any]:
    """Queue depth, oldest pending age, breaches, and the decision mix.

    This is the payload behind `GET /reviews/stats`, and every number here is a
    `SELECT` rather than a counter the process keeps, for the reason given in
    `fraud_rate`: the table is the truth, and a queue-depth gauge that disagrees
    with it is worse than no gauge.
    """
    moment = now or utcnow()
    by_status = await session.execute(
        select(ReviewQueueItem.status, func.count()).group_by(ReviewQueueItem.status)  # type: ignore[misc]
    )
    counts = {str(status): int(count) for status, count in by_status.all()}

    oldest = await session.execute(
        select(func.min(ReviewQueueItem.sla_deadline)).where(  # type: ignore[arg-type]
            ReviewQueueItem.status == ReviewStatus.PENDING
        )
    )
    oldest_deadline = oldest.scalar_one()
    oldest_age = (moment - oldest_deadline).total_seconds() if oldest_deadline else 0.0

    breached = await session.execute(
        select(func.count())  # type: ignore[arg-type]
        .select_from(ReviewQueueItem)
        .where(
            ReviewQueueItem.status == ReviewStatus.PENDING, ReviewQueueItem.sla_deadline < moment
        )
    )
    return {
        "by_status": counts,
        "pending": counts.get("pending", 0),
        "oldest_pending_age_seconds": max(0.0, oldest_age),
        "sla_breached": int(breached.scalar_one()),
    }


# --------------------------------------------------------------------------
# Notifications (outbox)
# --------------------------------------------------------------------------


async def enqueue_notification(
    session: AsyncSession, *, order_id: str, channel: str, template: str, body: str
) -> Notification:
    """Write an outbound notification in the caller's transaction.

    Written, not sent. Delivery is a worker's job, which is why a crash between
    "approved" and "customer told" leaves a row with `sent_at IS NULL` rather
    than a customer who was never notified and no record that they should have
    been.
    """
    notification = Notification(
        order_id=order_id,
        channel=channel,
        template=template,
        body=body,
        created_at=utcnow(),
    )
    session.add(notification)
    await session.flush()
    return notification


async def unsent_notifications(session: AsyncSession, *, limit: int = 100) -> list[Notification]:
    result = await session.execute(
        select(Notification)
        .where(Notification.sent_at.is_(None))
        .order_by(Notification.created_at)
        .limit(limit)
    )
    return list(result.scalars())


async def mark_notification_sent(session: AsyncSession, notification_id: int) -> None:
    await session.execute(
        update(Notification).where(Notification.id == notification_id).values(sent_at=utcnow())
    )


# --------------------------------------------------------------------------
# Audit
# --------------------------------------------------------------------------


async def record_audit(
    session: AsyncSession,
    *,
    order_id: str | None,
    entity: str,
    entity_id: str,
    action: str,
    actor: str,
    detail: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> AuditLog:
    """Append one audit row.

    Insert-only by construction: nothing in this project updates or deletes a
    row here, and there is deliberately no `update_audit` for someone to reach
    for when a bug makes one look convenient.
    """
    entry = AuditLog(
        order_id=order_id,
        entity=entity,
        entity_id=entity_id,
        action=action,
        actor=actor,
        detail=detail or {},
        correlation_id=correlation_id,
        created_at=utcnow(),
    )
    session.add(entry)
    await session.flush()
    return entry


async def audit_for_order(session: AsyncSession, order_id: str) -> list[AuditLog]:
    """An order's whole history, oldest first. `GET /orders/{id}/timeline`."""
    result = await session.execute(
        select(AuditLog)
        .where(AuditLog.order_id == order_id)
        .order_by(AuditLog.created_at, AuditLog.id)
    )
    return list(result.scalars())


async def recent_audit(
    session: AsyncSession, *, limit: int = 100, action: str | None = None
) -> list[AuditLog]:
    statement = select(AuditLog).order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
    if action is not None:
        statement = statement.where(AuditLog.action == action)
    result = await session.execute(statement.limit(limit))
    return list(result.scalars())


def order_status_for_decision(decision: ReviewStatus) -> OrderStatus:
    """The order status a review decision implies.

    One function, because the mapping from a review decision to an order status
    is exactly the kind of pair that gets written twice with one of them wrong.
    """
    if decision == ReviewStatus.APPROVED:
        return OrderStatus.APPROVED
    if decision == ReviewStatus.REJECTED:
        return OrderStatus.REJECTED
    if decision == ReviewStatus.EXPIRED:
        return OrderStatus.CANCELLED
    return OrderStatus.SCORED


def reservation_state_for_decision(decision: ReviewStatus) -> ReservationState:
    """The reservation transition a review decision implies."""
    return (
        ReservationState.COMMITTED
        if decision == ReviewStatus.APPROVED
        else ReservationState.RELEASED
    )


async def set_order_status_for_review(
    session: AsyncSession, *, order_id: str, decision: ReviewStatus
) -> None:
    """Move the order to the status a review decision implies.

    Thin wrapper over `orders.set_order_status` that imports it lazily, because
    `orders` and `reviews` both need each other's decision mapping and a
    module-level cycle would make one of them import partially.
    """
    from backend.database.repositories import orders as order_repo

    await order_repo.set_order_status(
        session, order_id=order_id, status=order_status_for_decision(decision)
    )
