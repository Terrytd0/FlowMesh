"""Order ingress and reads.

`POST /orders` is the sprint's public surface and the endpoint the load test
drives, so its ordering is deliberate:

1. rate limit (cheapest rejection first)
2. idempotency cache lookup (microseconds)
3. persist the order
4. publish `order.accepted`
5. return 202

Steps 3 and 4 are separated on purpose, and the order matters. Publishing inside
the transaction would make the event visible to the order processor before the
order row exists -- the processor would score an order it cannot read. So the row
commits first, then the event publishes.

That leaves a window: the order exists but the event was never published, if the
process dies between the two. It is closed by two things rather than pretended
away -- `scripts/reconciliation.py` reports orders with no matching event, and
the API's idempotency key means the client's retry re-runs the whole path. The
alternative (publish, then insert) inverts the failure into "an event for an order
that does not exist", which no consumer can resolve.

**202, not 201.** The order is accepted; it is not fulfilled, not scored, not
approved. A 201 here would tell the client the work is done, and the status
endpoint exists precisely because that is not true yet.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy.exc import IntegrityError

from backend.api.ratelimit import RateLimiter
from backend.api.schemas import (
    FraudScoreOut,
    LineItemOut,
    OrderAcceptedResponse,
    OrderCreateRequest,
    OrderOut,
    TimelineEntry,
)
from backend.auth.dependencies import CurrentPrincipal, DbSession
from backend.core.clock import utcnow
from backend.core.ids import new_correlation_id, new_order_id
from backend.core.logging import get_logger
from backend.database.repositories import fraud as fraud_repo
from backend.database.repositories import orders as order_repo
from backend.database.repositories import reviews as review_repo
from backend.events.schema import (
    EventType,
    LineItem,
    OrderAcceptedPayload,
    OrderStatus,
    PaymentDetails,
    Topic,
    envelope_for,
)

logger = get_logger(__name__)

router = APIRouter(tags=["orders"])

#: Status returned for an accepted order. 202, not 201: see the module docstring.
ACCEPTED = status.HTTP_202_ACCEPTED


def get_rate_limiter(request: Request) -> RateLimiter:
    """The limiter the app built at startup.

    Read off `app.state` rather than constructed per request: a per-request
    limiter is a per-request Redis connection pool, which at 500/sec is 500
    pools.
    """
    limiter: RateLimiter = request.app.state.rate_limiter
    return limiter


def get_idempotency_cache(request: Request) -> Any:
    return request.app.state.idempotency_cache


def get_event_bus(request: Request) -> Any:
    """The `EventBus` the app built at startup.

    Every long-lived dependency is on `app.state`, built once in the lifespan
    hook. Constructing a bus inside a route would create a Kafka producer per
    request, and the second one would fail to connect while the first still held
    the connection.
    """
    return request.app.state.event_bus


RateLimiterDep = Annotated[RateLimiter, Depends(get_rate_limiter)]
IdempotencyDep = Annotated[Any, Depends(get_idempotency_cache)]
EventBusDep = Annotated[Any, Depends(get_event_bus)]


@router.post("/orders", response_model=OrderAcceptedResponse, status_code=ACCEPTED)
async def create_order(
    request: OrderCreateRequest,
    response: Response,
    session: DbSession,
    principal: CurrentPrincipal,
    limiter: RateLimiterDep,
    idem_cache: IdempotencyDep,
    bus: EventBusDep,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> OrderAcceptedResponse:
    """Accept an order and publish it for scoring.

    The `Idempotency-Key` header is what makes a client retry safe. Without one
    the server generates a key from the connection, which makes retries
    impossible -- and a retried order is a double charge, which is the single
    worst outcome this endpoint has.
    """
    limit = await limiter.check(principal.subject)
    response.headers["X-RateLimit-Limit"] = str(limit.limit)
    response.headers["X-RateLimit-Remaining"] = str(limit.remaining)
    if not limit.allowed:
        # A 429 without a `Retry-After` is a client that has to guess. The header
        # is set on the raised `HTTPException` rather than on the injected
        # `Response` object, because raising builds a *new* response and anything
        # set on the injected one is discarded -- which is why the first version of
        # this returned a 429 with no `Retry-After` at all.
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={
                "Retry-After": str(limit.reset_seconds),
                "X-RateLimit-Limit": str(limit.limit),
                "X-RateLimit-Remaining": "0",
            },
        )

    key = idempotency_key or f"auto:{principal.subject}:{new_correlation_id()}"
    cached = await idem_cache.get(key)
    if cached is not None:
        existing = await order_repo.get_order(session, cached)
        if existing is not None:
            response.headers["Idempotent-Replay"] = "true"
            return OrderAcceptedResponse(
                order_id=existing.id,
                status=existing.status,
                correlation_id=existing.correlation_id,
                idempotent_replay=True,
                held_reason=_public_hold_reason(existing.status, existing.hold_reason),
            )

    existing = await order_repo.get_order_by_idempotency_key(session, key)
    if existing is not None:
        response.headers["Idempotent-Replay"] = "true"
        await idem_cache.put(key, existing.id)
        return OrderAcceptedResponse(
            order_id=existing.id,
            status=existing.status,
            correlation_id=existing.correlation_id,
            idempotent_replay=True,
            held_reason=_public_hold_reason(existing.status, existing.hold_reason),
        )

    order_id = new_order_id()
    correlation_id = new_correlation_id()

    # Merge duplicate SKUs before anything is written or published. A client that
    # sends `[{SKU-A, 1}, {SKU-A, 2}]` means three of SKU-A, and `order_items`
    # enforces `UNIQUE (order_id, sku)` -- so without this the request fails with
    # a 500 on a completely legitimate basket. Raises `ValueError` for two lines of
    # one SKU at different prices, which is an inconsistent request rather than a
    # duplicate.
    try:
        items = request.merged_items()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    total_cents = (
        request.total_cents
        if request.total_cents is not None
        else sum(item.unit_price_cents * item.quantity for item in items)
    )

    # Step 3: persist. Autoflush is off, so this is the explicit flush.
    async with session.begin_nested():
        try:
            await order_repo.insert_order(
                session,
                order_id=order_id,
                customer_id=request.customer_id,
                total_cents=total_cents,
                correlation_id=correlation_id,
                idempotency_key=key,
                items=[item.model_dump() for item in items],
            )
        except IntegrityError:
            # Lost the race with a concurrent identical request. The other one won,
            # and returning its order is the correct answer, not an error.
            await session.rollback()
            duplicate = await order_repo.get_order_by_idempotency_key(session, key)
            if duplicate is None:
                raise
            response.headers["Idempotent-Replay"] = "true"
            return OrderAcceptedResponse(
                order_id=duplicate.id,
                status=duplicate.status,
                correlation_id=duplicate.correlation_id,
                idempotent_replay=True,
                held_reason=_public_hold_reason(duplicate.status, duplicate.hold_reason),
            )

    await idem_cache.put(key, order_id)

    # Step 4: publish, after the row is durable.
    payload = OrderAcceptedPayload(
        order_id=order_id,
        customer_id=request.customer_id,
        items=[
            LineItem(
                sku=item.sku,
                quantity=item.quantity,
                unit_price_cents=item.unit_price_cents,
            )
            for item in items
        ],
        payment=PaymentDetails(**request.payment.model_dump()),
        total_cents=total_cents,
        idempotency_key=key,
        received_at=utcnow(),
    )
    event = envelope_for(
        EventType.ORDER_ACCEPTED, payload, order_id=order_id, correlation_id=correlation_id
    )

    try:
        await bus.publish(Topic.ORDER, order_id, event)
    except Exception as exc:
        # The order exists and the event did not publish. Reported loudly and
        # audited, because this is the window the reconciliation report exists to
        # find, and a silent 500 here would leave an order nobody can explain.
        logger.error("order %s persisted but not published: %s", order_id, exc)
        async with session.begin_nested():
            await review_repo.record_audit(
                session,
                order_id=order_id,
                entity="order",
                entity_id=order_id,
                action="publish_failed",
                actor="api:orders",
                detail={"error": f"{type(exc).__name__}: {exc}"[:200]},
                correlation_id=correlation_id,
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "order accepted but not queued for processing; retry with the same Idempotency-Key"
            ),
        ) from exc

    logger.info(
        "accepted order_id=%s customer_id=%s total_cents=%s",
        order_id,
        request.customer_id,
        total_cents,
    )
    return OrderAcceptedResponse(
        order_id=order_id,
        status=OrderStatus.ACCEPTED,
        correlation_id=correlation_id,
        idempotent_replay=False,
    )


@router.get("/orders/{order_id}", response_model=OrderOut)
async def get_order(order_id: str, session: DbSession, principal: CurrentPrincipal) -> OrderOut:
    """One order, with its latest scoring decision.

    A customer may read their own order and nothing else. The check is a string
    comparison rather than a query filter, because the customer id is in the token
    and the order is a single row -- and because a filtered query that *forgot* to
    filter would return other people's orders, which is the failure this line
    exists to prevent.
    """
    order = await _readable_order(session, order_id, principal)

    items = await order_repo.list_order_items(session, order_id)
    score = await fraud_repo.latest_score(session, order_id)
    return OrderOut(
        order_id=order.id,
        customer_id=order.customer_id,
        status=order.status,
        total_cents=order.total_cents,
        items=[
            LineItemOut(
                sku=item.sku, quantity=item.quantity, unit_price_cents=item.unit_price_cents
            )
            for item in items
        ],
        fraud_score=(
            FraudScoreOut(
                score=score.score,
                band=score.band,
                model_version=score.model_version,
                reasons=list(score.reasons or []),
                rationale=score.rationale,
                degraded=score.degraded,
                llm_used=score.llm_used,
                latency_ms=score.latency_ms,
                decided_at=score.decided_at,
            )
            if score is not None
            else None
        ),
        hold_reason=_public_hold_reason(order.status, order.hold_reason),
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


@router.get("/orders/{order_id}/timeline", response_model=list[TimelineEntry])
async def order_timeline(
    order_id: str, session: DbSession, principal: CurrentPrincipal
) -> list[TimelineEntry]:
    """The order's audit trail, oldest first.

    This is the endpoint that answers "what happened to my order" for a support
    agent, and it exists because reconstructing an order's history from the event
    log requires the log, which an agent does not have.
    """
    # Loaded and discarded: the call is the authorisation check. `_readable_order`
    # raises 404 for an order that does not exist *or* is not the caller's, which is
    # the whole point of having it.
    _ = await _readable_order(session, order_id, principal)

    entries = await review_repo.audit_for_order(session, order_id)
    return [
        TimelineEntry(
            created_at=entry.created_at,
            entity=entry.entity,
            entity_id=entry.entity_id,
            action=entry.action,
            actor=entry.actor,
            detail=dict(entry.detail or {}),
        )
        for entry in entries
    ]


@router.get("/orders", response_model=dict)
async def list_orders(session: DbSession, principal: CurrentPrincipal) -> dict[str, Any]:
    """Order counts by status. Supervisor/admin only.

    A summary rather than a listing: the "show me every order" screen belongs to
    an ops tool with pagination and a query language, and a portfolio API that
    returns 100k rows is a portfolio API that gets one line of feedback from
    whoever tried it.
    """
    if not principal.is_supervisor:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="supervisor role required"
        )
    counts = await order_repo.count_by_status(session)
    return {"by_status": counts, "total": sum(counts.values())}


async def _readable_order(session: Any, order_id: str, principal: Any) -> Any:
    """Load an order the caller is allowed to read, or raise 404.

    **One comparison, and it is only the comparison.** The original check here was

        order.customer_id not in (principal.subject, "CUST-1")

    and the `"CUST-1"` was a demo convenience that turned into an authorization
    bypass: with the seeded customer being `CUST-1`, *every* authenticated customer
    could read `CUST-1`'s orders. It survived because the suite runs with
    `FLOWMESH_AUTH_ENABLED=false`, so every request was already an anonymous
    supervisor and the check never ran -- the first test to exercise it with real
    auth
    (`tests/integration/test_orders_api.py::test_a_customer_cannot_read_another_customers_order`)
    read a 200 and failed.

    **404 rather than 403 for both "no such order" and "not yours".** A 403 on an
    existing order confirms it exists, which is a free existence oracle for
    enumerating order ids and, more usefully, tells an attacker which of their
    guessed ids are real.

    Supervisors and admins see everything, because the review queue and the ops
    dashboard both need to.
    """
    order = await order_repo.get_order(session, order_id)
    if order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="order not found")
    if not principal.is_supervisor and order.customer_id != principal.subject:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="order not found")
    return order


def _public_hold_reason(order_status: str, hold_reason: str | None) -> str | None:
    """What a customer is told about a held order.

    Never the model internals. A caller that learns "your order scored 0.72
    because the card country differed from the shipping country" learns exactly
    which features to evade, which turns the fraud model into an attack surface
    with a published manual.
    """
    if hold_reason is None:
        return None
    if str(order_status) in ("approved", "rejected", "cancelled"):
        return None
    return "order held for review"
