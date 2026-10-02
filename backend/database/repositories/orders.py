"""Order persistence.

Nothing here decides anything -- routing, scoring and inventory policy live above
this layer. These functions read and write, and the audit rows they leave behind
are the reason the pipeline can be reconstructed after an incident.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.clock import utcnow
from backend.database.models import AuditLog, Order, OrderItem, StockReservation
from backend.events.schema import OrderStatus, ReservationState, RiskBand


async def get_order(session: AsyncSession, order_id: str) -> Order | None:
    result = await session.execute(select(Order).where(Order.id == order_id))
    return result.scalar_one_or_none()


async def get_order_by_idempotency_key(session: AsyncSession, key: str) -> Order | None:
    """Look an order up by the caller's Idempotency-Key.

    The retry path. A mobile client that times out and re-POSTs must get its
    original order back, and the only thing that makes the two requests
    recognisably the same is a key the caller supplied.
    """
    result = await session.execute(select(Order).where(Order.idempotency_key == key))
    return result.scalar_one_or_none()


async def list_order_items(session: AsyncSession, order_id: str) -> list[OrderItem]:
    result = await session.execute(
        select(OrderItem).where(OrderItem.order_id == order_id).order_by(OrderItem.id)
    )
    return list(result.scalars())


async def insert_order(
    session: AsyncSession,
    *,
    order_id: str,
    customer_id: str,
    total_cents: int,
    correlation_id: str,
    idempotency_key: str,
    items: list[dict[str, Any]],
    actor: str = "api:orders",
) -> Order:
    """Create the order and its lines.

    Raises `IntegrityError` on a duplicate idempotency key, which the API turns
    into "here is the order you already made" rather than a 500. A retried POST
    that creates a second order is the worst failure this endpoint has.
    """
    order = Order(
        id=order_id,
        customer_id=customer_id,
        status=OrderStatus.ACCEPTED,
        total_cents=total_cents,
        correlation_id=correlation_id,
        idempotency_key=idempotency_key,
    )
    session.add(order)
    # The audit row is written here, in the same transaction, rather than by the
    # caller. Acceptance is the first state transition an order goes through, so
    # an order row with no audit entry is a hole in the trail from the moment it is
    # created -- and `GET /orders/{id}/timeline` then answers "what happened to my
    # order?" with an empty list. The order processor's `accepted` row comes later
    # and records that the *event* was handled, which is a different fact.
    session.add(
        AuditLog(
            order_id=order_id,
            entity="order",
            entity_id=order_id,
            action="accepted",
            actor=actor,
            detail={
                "customer_id": customer_id,
                "total_cents": total_cents,
                "items": len(items),
                "correlation_id": correlation_id,
            },
            correlation_id=correlation_id,
            created_at=utcnow(),
        )
    )
    for item in items:
        session.add(
            OrderItem(
                order_id=order_id,
                sku=item["sku"],
                quantity=int(item["quantity"]),
                unit_price_cents=int(item.get("unit_price_cents", 0)),
            )
        )
    await session.flush()
    return order


async def set_order_status(
    session: AsyncSession,
    *,
    order_id: str,
    status: OrderStatus,
    hold_reason: str | None = None,
    fraud_score: float | None = None,
    fraud_band: RiskBand | None = None,
) -> None:
    """Move an order to a new status.

    `hold_reason` is cleared on a terminal status, so a rejected order does not
    keep advertising why it was once held. Small, and the difference between a
    dashboard that is right and one that quietly accumulates stale explanations.
    """
    values: dict[str, Any] = {"status": status, "updated_at": utcnow()}
    if status in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
        values["hold_reason"] = None
    elif hold_reason is not None:
        values["hold_reason"] = hold_reason
    if fraud_score is not None:
        values["fraud_score"] = fraud_score
    if fraud_band is not None:
        values["fraud_band"] = fraud_band

    statement = update(Order).where(Order.id == order_id).values(**values)
    await session.execute(statement)


async def count_orders(session: AsyncSession) -> int:
    result = await session.execute(select(func.count()).select_from(Order))
    return int(result.scalar_one())


async def count_by_status(session: AsyncSession) -> dict[str, int]:
    """Order counts per status, for `/healthz` and the load-test report."""
    result = await session.execute(
        select(Order.status, func.count()).group_by(Order.status)  # type: ignore[misc]
    )
    return {str(status): int(count) for status, count in result.all()}


async def orders_needing_stock(session: AsyncSession, limit: int = 100) -> list[Order]:
    """Orders that were scored but have no reservation -- the shortfall repair list.

    A reservation that failed for a transient reason (a lock timeout, a failover)
    leaves an order in `scored` forever. This is the query an operator runs to
    find them, and it is why the pipeline has a retry path rather than only a
    failure path.
    """
    from sqlalchemy import exists

    reserved = exists(
        select(StockReservation.id).where(
            StockReservation.order_id == Order.id,
            StockReservation.state == ReservationState.HELD,
        )
    )
    result = await session.execute(
        select(Order).where(Order.status == OrderStatus.SCORED, ~reserved).limit(limit)
    )
    return list(result.scalars())
