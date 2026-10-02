"""Inventory service: the only writer of stock state.

The repository holds the SQL; this module holds the *decisions* -- which warehouse
serves an order, what happens when there is not enough, and the reservation
lifecycle. Keeping the two apart is what lets the SQL be reviewed as SQL and the
policy be reviewed as policy.

**Transaction scope is the load-bearing detail here.** On PostgreSQL the row lock
taken by `reserve_units` is held until the end of the transaction, so anything
slow inside this transaction serialises every other reservation for the same SKU.
`reserve()` therefore does the smallest possible work: conditional decrement,
insert the reservation row, publish *after* the commit. Nothing here does I/O to
another service while holding the lock.

The ordering that follows from that is the interesting part. Publishing the
`inventory.reserved` event inside the transaction would mean the event is visible
before the commit, and a consumer that acts on it before this transaction lands
reserves stock against a hold that does not exist. So:

1. conditional decrement + reservation row, committed
2. *then* publish

The cost of that ordering is a window where the stock is held and the event has
not been published. It is closed by the reconciliation in
`scripts/chaos_test.py`, which compares published events against applied ones and
reports the difference. A pipeline that cannot be reconciled is a pipeline whose
failures are invisible, and this is the trade the sprint makes deliberately.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.clock import utcnow
from backend.core.logging import get_logger
from backend.database.repositories import inventory as repo
from backend.events.schema import (
    EventType,
    InventoryCommittedPayload,
    InventoryLine,
    InventoryReleasedPayload,
    InventoryReservedPayload,
    InventoryShortfallPayload,
    ReservationState,
    Topic,
    envelope_for,
)
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)


@dataclass(frozen=True)
class ReservationOutcome:
    """What happened when stock was requested for an order.

    `reserved` is `False` with `reason` set when the order could not be served --
    and the *caller* decides what to do about it (try another warehouse, tell the
    customer, waitlist). The service reports; it does not choose the customer's
    experience.
    """

    reserved: bool
    reservation_id: str | None = None
    warehouse_id: str | None = None
    reason: str = ""
    shortfalls: list[tuple[str, int]] = field(default_factory=list)


class InventoryService:
    """Reservation lifecycle across the twelve warehouses."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        metrics: FlowMeshMetrics,
        ttl_seconds: int = 900,
    ) -> None:
        self._session = session
        self._metrics = metrics
        self._ttl = ttl_seconds

    # -- reserving ---------------------------------------------------------

    async def reserve_for_order(
        self,
        *,
        order_id: str,
        lines: list[dict[str, Any]],
        warehouse_id: str | None = None,
    ) -> ReservationOutcome:
        """Hold stock for every line of an order.

        All-or-nothing across the order's lines. Partial reservation is worse
        than none: the customer is told two of four items are coming, and the
        other two are silently unavailable with no order to explain why. So if
        any line fails, everything already held in this call is released before
        returning -- inside the same transaction, so a failure part-way leaves no
        trace at all.
        """
        target = warehouse_id or await self._pick_warehouse(lines)
        if target is None:
            # Counted here, and this is the reason the branch exists at all: a
            # request for more units than *any* warehouse holds is the most
            # severe oversell case, and before this it was the one case that went
            # uncounted -- the order returned "no warehouse holds these items"
            # without ever incrementing `oversell_rejections`. The dashboard said
            # the oversell rate was zero while orders were being refused.
            #
            # Attributed to every warehouse that was considered, since "nowhere"
            # is not a warehouse and an unattributed rejection cannot be split
            # per site for a supply-chain conversation.
            considered = await self._warehouses_holding({line["sku"] for line in lines})
            for warehouse in considered or ["none"]:
                self._metrics.oversell_rejections.labels(warehouse=warehouse).inc()
            logger.warning(
                "no warehouse can serve order_id=%s requested=%s",
                order_id,
                [(line["sku"], int(line["quantity"])) for line in lines],
            )
            return ReservationOutcome(
                reserved=False,
                reason="no warehouse holds these items",
                shortfalls=[(line["sku"], int(line["quantity"])) for line in lines],
            )

        shortfalls: list[tuple[str, int]] = []
        held: list[tuple[str, int]] = []
        for line in lines:
            sku = line["sku"]
            quantity = int(line["quantity"])
            if not await repo.reserve_units(
                self._session, warehouse_id=target, sku=sku, quantity=quantity
            ):
                shortfalls.append((sku, quantity))
                continue
            held.append((sku, quantity))

        if shortfalls:
            # Roll the partial hold back inside this transaction.
            for sku, quantity in held:
                await repo.release_units(
                    self._session, warehouse_id=target, sku=sku, quantity=quantity
                )
            # One rejection per reservation attempt, labelled by the warehouse
            # that refused. The per-SKU detail is in `shortfalls` on the outcome
            # and in the log line below, deliberately not in a metric label: a
            # per-SKU label would put tens of thousands of series into the
            # time-series store for a counter nobody slices by SKU.
            self._metrics.oversell_rejections.labels(warehouse=target).inc()
            logger.info(
                "reservation refused order_id=%s warehouse=%s shortfalls=%s",
                order_id,
                target,
                shortfalls,
            )
            return ReservationOutcome(
                reserved=False,
                warehouse_id=target,
                reason="insufficient stock",
                shortfalls=shortfalls,
            )

        reservation = await repo.create_reservation(
            self._session,
            order_id=order_id,
            warehouse_id=target,
            ttl_seconds=self._ttl,
            # Snapshot the lines onto the hold. Without this the release path has
            # to join `order_items`, and a reservation whose order rows are not
            # (yet) visible releases nothing while reporting success.
            lines=lines,
        )
        # Units, not reservations: the gauge an operator reads is "how much stock
        # did we promise", and a per-reservation counter would make a 10-unit order
        # look identical to a 1-unit one.
        for _sku, quantity in held:
            self._metrics.stock_reserved.labels(warehouse=target).inc(quantity)
        logger.info(
            "reserved order_id=%s warehouse=%s lines=%s reservation=%s",
            order_id,
            target,
            held,
            reservation.id,
        )
        return ReservationOutcome(
            reserved=True,
            reservation_id=reservation.id,
            warehouse_id=target,
            shortfalls=[],
        )

    async def _warehouses_holding(self, skus: set[str]) -> list[str]:
        """Which warehouses hold any of these SKUs, for attributing a refusal.

        A refusal with no warehouse attached cannot be split per site, which is
        the first question a supply-chain team asks: "which warehouse is out?"
        """
        if not skus:
            return []
        rows = await repo.list_inventory(self._session, sku=sorted(skus)[0])
        return sorted({row.warehouse_id for row in rows})

    async def _pick_warehouse(self, lines: list[dict[str, Any]]) -> str | None:
        """The warehouse with the most headroom across the order's lines.

        "Most headroom" rather than "first with enough", because choosing the
        first one that fits packs one warehouse empty and leaves the next order
        unservable -- the classic way a greedy allocator turns a distribution
        problem into a stockout problem. Ties break on the warehouse id so the
        choice is deterministic and a replay picks the same warehouse.
        """
        best: tuple[int, str] | None = None
        for line in lines:
            sku = line["sku"]
            quantity = int(line["quantity"])
            rows = await repo.list_inventory(self._session, sku=sku)
            for row in rows:
                headroom = row.available - quantity
                if headroom < 0:
                    continue
                if (
                    best is None
                    or headroom > best[0]
                    or (headroom == best[0] and row.warehouse_id < best[1])
                ):
                    best = (headroom, row.warehouse_id)
        return best[1] if best else None

    # -- lifecycle transitions --------------------------------------------

    async def commit_reservation(self, *, order_id: str, warehouse_id: str) -> bool:
        """Turn a hold into shipped stock. Called after a review approves."""
        reservation = await repo.get_reservation(
            self._session, order_id=order_id, warehouse_id=warehouse_id
        )
        if reservation is None:
            return False
        if reservation.state != ReservationState.HELD:
            # Already committed or released: a redelivered approval, not an error.
            return False
        lines = repo.reservation_lines(reservation) or await repo.reserved_units(
            self._session, reservation
        )
        for sku, quantity in lines:
            if not await repo.commit_units(
                self._session, warehouse_id=warehouse_id, sku=sku, quantity=quantity
            ):
                logger.warning(
                    "commit refused (reserved too low) order_id=%s sku=%s qty=%s",
                    order_id,
                    sku,
                    quantity,
                )
                return False
        await repo.set_reservation_state(
            self._session,
            reservation_id=reservation.id,
            state=ReservationState.COMMITTED,
            reason="approved",
        )
        return True

    async def release_reservation(self, *, order_id: str, warehouse_id: str, reason: str) -> bool:
        """Return held stock to available. Called on rejection or expiry."""
        reservation = await repo.get_reservation(
            self._session, order_id=order_id, warehouse_id=warehouse_id
        )
        if reservation is None or reservation.state != ReservationState.HELD:
            return False
        lines = repo.reservation_lines(reservation) or await repo.reserved_units(
            self._session, reservation
        )
        for sku, quantity in lines:
            await repo.release_units(
                self._session, warehouse_id=warehouse_id, sku=sku, quantity=quantity
            )
            self._metrics.stock_released.labels(warehouse=warehouse_id).inc(quantity)
        await repo.set_reservation_state(
            self._session,
            reservation_id=reservation.id,
            state=ReservationState.RELEASED,
            reason=reason,
            released_at=utcnow(),
        )
        logger.info("released order_id=%s warehouse=%s reason=%s", order_id, warehouse_id, reason)
        return True

    async def expire_reservation(self, reservation_id: str) -> bool:
        """Sweeper path: release a hold whose TTL has passed."""
        from sqlalchemy import select

        from backend.database.models import StockReservation

        result = await self._session.execute(
            select(StockReservation).where(StockReservation.id == reservation_id)
        )
        reservation = result.scalar_one_or_none()
        if reservation is None or reservation.state != ReservationState.HELD:
            return False
        lines = repo.reservation_lines(reservation) or await repo.reserved_units(
            self._session, reservation
        )
        for sku, quantity in lines:
            await repo.release_units(
                self._session, warehouse_id=reservation.warehouse_id, sku=sku, quantity=quantity
            )
            self._metrics.stock_released.labels(warehouse=reservation.warehouse_id).inc(quantity)
        await repo.set_reservation_state(
            self._session,
            reservation_id=reservation.id,
            state=ReservationState.EXPIRED,
            reason="ttl_expired",
            released_at=utcnow(),
        )
        self._metrics.reservation_expirations.inc()
        return True

    async def sweep_expired(self, *, limit: int = 200) -> int:
        """Release every hold past its TTL. Returns how many were reclaimed."""
        expired = await repo.expired_reservations(self._session, limit=limit)
        reclaimed = 0
        for reservation in expired:
            if await self.expire_reservation(reservation.id):
                reclaimed += 1
        # On every pass, whether or not anything was reclaimed. The heartbeat has
        # to be unconditional: an alert that reads "the sweeper found nothing" can
        # never distinguish a quiet night from a dead sweeper, which is exactly the
        # failure it exists to catch. `time() - ..._last_success_unixtime` can.
        self._metrics.reservation_sweep_last_success.set_to_current_time()
        if reclaimed:
            logger.info("swept %s expired reservations", reclaimed)
        return reclaimed

    # -- event payloads ----------------------------------------------------

    def reserved_event(
        self,
        *,
        order_id: str,
        outcome: ReservationOutcome,
        lines: list[dict[str, Any]],
        correlation_id: str,
    ) -> Any:
        """The `inventory.reserved` envelope for a successful hold."""
        from backend.core.ids import new_event_id

        payload = InventoryReservedPayload(
            order_id=order_id,
            warehouse_id=outcome.warehouse_id or "",
            lines=[
                InventoryLine(sku=str(line["sku"]), quantity=int(line["quantity"]))
                for line in lines
            ],
            reservation_id=outcome.reservation_id or new_event_id(),
            expires_at=utcnow() + timedelta(seconds=self._ttl),
        )
        return envelope_for(
            EventType.INVENTORY_RESERVED,
            payload,
            order_id=order_id,
            correlation_id=correlation_id,
        )

    def shortfall_event(
        self,
        *,
        order_id: str,
        outcome: ReservationOutcome,
        lines: list[dict[str, Any]],
        correlation_id: str,
    ) -> Any:
        """The `inventory.shortfall` envelope for a refusal.

        Prefers the *actual* shortfalls over the requested lines, because "which
        lines could not be served" is what a supply-chain team needs and the full
        request list is only a fallback for the case where the refusal came before
        any line was tried.
        """
        requested = [
            InventoryLine(sku=sku, quantity=quantity) for sku, quantity in outcome.shortfalls
        ] or [InventoryLine(sku=str(line["sku"]), quantity=int(line["quantity"])) for line in lines]
        payload = InventoryShortfallPayload(
            order_id=order_id,
            warehouse_id=outcome.warehouse_id or "none",
            requested=requested,
            reason=outcome.reason or "insufficient stock",
        )
        return envelope_for(
            EventType.INVENTORY_SHORTFALL,
            payload,
            order_id=order_id,
            correlation_id=correlation_id,
        )

    def released_event(
        self,
        *,
        order_id: str,
        warehouse_id: str,
        reservation_id: str,
        reason: str,
        correlation_id: str,
    ) -> Any:
        payload = InventoryReleasedPayload(
            order_id=order_id,
            warehouse_id=warehouse_id,
            reservation_id=reservation_id,
            reason=reason,
        )
        return envelope_for(
            EventType.INVENTORY_RELEASED,
            payload,
            order_id=order_id,
            correlation_id=correlation_id,
        )

    def committed_event(
        self, *, order_id: str, warehouse_id: str, reservation_id: str, correlation_id: str
    ) -> Any:
        payload = InventoryCommittedPayload(
            order_id=order_id, warehouse_id=warehouse_id, reservation_id=reservation_id
        )
        return envelope_for(
            EventType.INVENTORY_COMMITTED,
            payload,
            order_id=order_id,
            correlation_id=correlation_id,
        )


def inventory_topics() -> tuple[str, str]:
    """The topic names, for callers that must not import the schema directly."""
    return str(Topic.INVENTORY), str(Topic.ORDER)
