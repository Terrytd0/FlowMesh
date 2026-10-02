"""The review dashboard: see what is held, and decide it.

Every route here requires a supervisor role. That is the difference between this
API and Sprint 4's: authentication gets you in, authorisation decides whether you
can reject a customer's order.

`POST /reviews/{id}/approve` and `/reject` do not write the decision themselves.
They record it in `review_queue_items` and publish a task to `review-queue`; the
`ReviewWorker` applies it and performs the inventory transition. The reason is
consistency: the decision and its inventory effect happen in one transaction, in
one place, whether the decision arrives from a human or from any other producer.
An API route that both decided and applied would be a second implementation of
the transition, and the two would diverge exactly when it mattered.

What the API does own is the SLA arithmetic, because the dashboard and the
warning log must not compute "is this late" differently.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.schemas import (
    ReviewDecisionRequest,
    ReviewItemOut,
    ReviewStatsOut,
)
from backend.auth.dependencies import DbSession, SupervisorPrincipal
from backend.core.clock import utcnow
from backend.core.logging import get_logger
from backend.database.models import ReviewQueueItem
from backend.database.repositories import reviews as review_repo
from backend.events.schema import ReviewStatus
from backend.queues.memory_queue import json_task

logger = get_logger(__name__)

router = APIRouter(prefix="/reviews", tags=["reviews"])


@router.get("", response_model=list[ReviewItemOut])
async def list_reviews(
    session: DbSession,
    principal: SupervisorPrincipal,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    status_filter: Annotated[ReviewStatus | None, Query(alias="status")] = None,
) -> list[ReviewItemOut]:
    """Pending reviews, oldest first.

    Default `status_filter` is `pending` because that is the only list a
    reviewer opens. Showing decided items mixed in would push the actionable work
    below the fold, and the filter exists for when someone wants history.
    """
    _ = principal
    if status_filter is None:
        items = await review_repo.pending_reviews(session, limit=limit)
    else:
        items = await _by_status(session, status_filter, limit)
    return [_to_out(item) for item in items]


async def _by_status(
    session: AsyncSession, status_value: ReviewStatus, limit: int
) -> list[ReviewQueueItem]:
    from sqlalchemy import select

    result = await session.execute(
        select(ReviewQueueItem)
        .where(ReviewQueueItem.status == status_value)
        .order_by(ReviewQueueItem.sla_deadline)
        .limit(limit)
    )
    return list(result.scalars())


@router.get("/stats", response_model=ReviewStatsOut)
async def review_stats(session: DbSession, principal: SupervisorPrincipal) -> ReviewStatsOut:
    """Queue depth, oldest pending age, and breaches.

    Computed by `SELECT`s in the repository rather than read off the
    `flowmesh_queue_depth` gauge. The gauge is for alerting on; this is for the
    screen in front of a person, and a person should never be shown a number that
    disagrees with the table because a scrape was missed.
    """
    _ = principal
    stats = await review_repo.review_stats(session)
    return ReviewStatsOut(**stats)


@router.get("/{review_id}", response_model=ReviewItemOut)
async def get_review(
    review_id: str, session: DbSession, principal: SupervisorPrincipal
) -> ReviewItemOut:
    _ = principal
    item = await review_repo.get_review_by_id(session, review_id)
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="review item not found")
    return _to_out(item)


def get_task_queue(request: Request) -> Any:
    """The `TaskQueue` the app built at startup. One per process, on `app.state`."""
    return request.app.state.task_queue


TaskQueueDep = Annotated[Any, Depends(get_task_queue)]


async def _decide(
    *,
    review_id: str,
    decision: ReviewStatus,
    body: ReviewDecisionRequest,
    session: AsyncSession,
    principal: Any,
    queue: Any,
) -> ReviewItemOut:
    """Record the decision request and hand the effect to the worker.

    Note what is *not* written here: the review item's own status. The API
    records that a decision was requested; the worker records that it was
    applied. Splitting it that way is what lets the human's answer and the
    inventory transition land in one transaction without this route owning the
    inventory logic.
    """
    item = await review_repo.get_review_by_id(session, review_id)
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="review item not found")
    if item.status != ReviewStatus.PENDING:
        # A repeat click, or a decision that already landed. Answering with the
        # existing decision and 200 is more useful than a 409, and it cannot be
        # mistaken for a new approval.
        logger.info("review %s already %s; returning existing", review_id, str(item.status))
        return _to_out(item)

    await review_repo.record_audit(
        session,
        order_id=item.order_id,
        entity="review",
        entity_id=review_id,
        action="decision_requested",
        actor=f"api:{principal.subject}",
        detail={"decision": decision.value, "note": body.note[:200]},
    )
    # Commit the audit before publishing, for the same reason the order route
    # commits before publishing: no consumer may observe an event before the state
    # it describes is durable.
    await session.commit()

    await queue.publish(
        _review_queue_name(queue),
        json_task(
            {
                "review_id": review_id,
                "order_id": item.order_id,
                "decision": decision.value,
                "note": body.note,
                "decided_by": principal.subject,
            }
        ),
        headers={"x-order-id": item.order_id, "x-review-id": review_id},
    )
    refreshed = await review_repo.get_review_by_id(session, review_id)
    return _to_out(refreshed if refreshed is not None else item)


def _review_queue_name(queue: Any) -> str:
    """The configured review-queue name, falling back to the protocol default."""
    return str(getattr(queue, "review_queue_name", None) or "review-queue")


@router.post("/{review_id}/approve", response_model=ReviewItemOut)
async def approve_review(
    review_id: str,
    body: ReviewDecisionRequest,
    session: DbSession,
    principal: SupervisorPrincipal,
    queue: TaskQueueDep,
) -> ReviewItemOut:
    """Approve a held order.

    The effect -- committing the reservation and notifying the customer -- is
    applied by `ReviewWorker`, not here. See the module docstring.
    """
    return await _decide(
        review_id=review_id,
        decision=ReviewStatus.APPROVED,
        body=body,
        session=session,
        principal=principal,
        queue=queue,
    )


@router.post("/{review_id}/reject", response_model=ReviewItemOut)
async def reject_review(
    review_id: str,
    body: ReviewDecisionRequest,
    session: DbSession,
    principal: SupervisorPrincipal,
    queue: TaskQueueDep,
) -> ReviewItemOut:
    """Reject a held order, releasing any reserved stock."""
    return await _decide(
        review_id=review_id,
        decision=ReviewStatus.REJECTED,
        body=body,
        session=session,
        principal=principal,
        queue=queue,
    )


def _to_out(item: ReviewQueueItem) -> ReviewItemOut:
    seconds = (item.sla_deadline - utcnow()).total_seconds()
    return ReviewItemOut(
        review_id=item.id,
        order_id=item.order_id,
        status=str(item.status),
        score=item.score,
        band=item.band,
        reasons=list(item.reasons or []),
        rationale=item.rationale,
        sla_deadline=item.sla_deadline,
        decided_by=item.decided_by,
        decided_at=item.decided_at,
        decision_note=item.decision_note,
        seconds_to_breach=seconds,
    )
