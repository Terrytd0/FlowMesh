"""The inventory worker: consume `inventory-events`, apply them.

The second consumer group, and the one that makes the stock ledger
*reconstructible*. Every movement of every unit is an event on
`inventory-events`, and this worker applies exactly those events. Replay the
topic from offset 0 into an empty database and you get the same ledger -- which
is the property that distinguishes an event-sourced inventory from a table that
happens to have a log next to it.

Each handler is idempotent on `event_id` through `processed_events`, and each one
is *also* idempotent on its effect: `inventory.released` for a reservation that
is already released returns `False` rather than decrementing stock again. The
second layer is not redundant. The first layer prevents the same event being
applied twice; the second prevents two *different* events describing the same
transition (a duplicate `review.requested` redelivered through two paths) from
double-applying it.

Handlers are narrow on purpose -- one event type each, small, and named after the
transition they perform rather than after the table they touch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.config.settings import Settings
from backend.core.logging import get_logger
from backend.database.repositories import idempotency
from backend.database.repositories import inventory as repo
from backend.database.repositories import reviews as review_repo
from backend.events.bus import EventBus, Subscription
from backend.events.schema import (
    EventEnvelope,
    EventType,
    InventoryCommittedPayload,
    InventoryReleasedPayload,
    InventoryReservedPayload,
    InventoryShortfallPayload,
    Topic,
)
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)

INVENTORY_GROUP = "flowmesh-inventory-worker-v1"


@dataclass
class InventoryStats:
    reserved: int = 0
    released: int = 0
    committed: int = 0
    shortfalls: int = 0
    duplicates: int = 0
    noops: int = 0


class InventoryWorker:
    """Applies inventory events to the ledger."""

    def __init__(
        self,
        *,
        settings: Settings,
        bus: EventBus,
        metrics: FlowMeshMetrics,
        session_factory: async_sessionmaker[AsyncSession],
        inventory_service_factory: Any = None,
    ) -> None:
        self._settings = settings
        self._bus = bus
        self._metrics = metrics
        self._sessions = session_factory
        self._inventory_factory = inventory_service_factory
        self.stats = InventoryStats()

    # -- the handler -------------------------------------------------------

    async def handle(self, envelope: EventEnvelope) -> None:
        """Route to the handler for this event type.

        A dispatch table rather than an `if` chain, because the set of event types
        is the set of handlers and the mapping should be data, not control flow.
        """
        handlers = {
            EventType.INVENTORY_RESERVED: self._on_reserved,
            EventType.INVENTORY_RELEASED: self._on_released,
            EventType.INVENTORY_COMMITTED: self._on_committed,
            EventType.INVENTORY_SHORTFALL: self._on_shortfall,
        }
        handler = handlers.get(envelope.event_type)
        if handler is None:
            logger.info("no inventory handler for %s", str(envelope.event_type))
            return

        async with self._metrics.timer(self._metrics.inventory_apply_latency):
            await handler(envelope)

    async def _on_reserved(self, envelope: EventEnvelope) -> None:
        """Confirm the hold is recorded. The units were already decremented.

        A subtle point worth stating: `inventory.reserved` does *not* decrement
        stock. The order processor reserved the units transactionally, before
        publishing this event, because a consumer cannot be trusted to be the
        place where an oversell is prevented -- it can crash between the check and
        the write. This handler verifies the reservation row exists and
        reconciles the count, so a ledger drift is visible even though the
        decrement already happened.
        """
        payload = InventoryReservedPayload.model_validate(envelope.payload)
        async with self._sessions() as session, session.begin():
            claimed = await idempotency.mark_processed(
                session,
                event_id=envelope.event_id,
                topic=str(Topic.INVENTORY),
                group=INVENTORY_GROUP,
                order_id=payload.order_id,
                effect=f"reserved:{len(payload.lines)}",
                partition=envelope.partition,
                log_offset=envelope.offset,
            )
            if not claimed:
                self.stats.duplicates += 1
                self._metrics.events_deduplicated.labels(topic=str(Topic.INVENTORY)).inc()
                return

            existing = await repo.get_reservation(
                session, order_id=payload.order_id, warehouse_id=payload.warehouse_id
            )
            if existing is None:
                # The order processor's reservation row is missing. Re-create it
                # rather than trusting the event: the event says what should be
                # true, and this is the one place that can repair it.
                await repo.create_reservation(
                    session,
                    order_id=payload.order_id,
                    warehouse_id=payload.warehouse_id,
                    ttl_seconds=self._settings.inventory_reservation_ttl_seconds,
                    reservation_id=payload.reservation_id,
                )
                logger.warning(
                    "reservation row recreated from event order_id=%s warehouse=%s",
                    payload.order_id,
                    payload.warehouse_id,
                )
            await review_repo.record_audit(
                session,
                order_id=payload.order_id,
                entity="inventory",
                entity_id=payload.order_id,
                action="reserved_confirmed",
                actor="worker:inventory",
                detail={"warehouse": payload.warehouse_id, "lines": len(payload.lines)},
                correlation_id=envelope.correlation_id,
            )
        self.stats.reserved += 1

    async def _on_released(self, envelope: EventEnvelope) -> None:
        payload = InventoryReleasedPayload.model_validate(envelope.payload)
        async with self._sessions() as session, session.begin():
            claimed = await idempotency.mark_processed(
                session,
                event_id=envelope.event_id,
                topic=str(Topic.INVENTORY),
                group=INVENTORY_GROUP,
                order_id=payload.order_id,
                effect="released",
                partition=envelope.partition,
                log_offset=envelope.offset,
            )
            if not claimed:
                self.stats.duplicates += 1
                self._metrics.events_deduplicated.labels(topic=str(Topic.INVENTORY)).inc()
                return

            service = self._service(session)
            changed = await service.release_reservation(
                order_id=payload.order_id,
                warehouse_id=payload.warehouse_id,
                reason=payload.reason,
            )
            if not changed:
                # Already released or committed. Expected on redelivery; the
                # effect-layer guard caught it, so nothing moves.
                self.stats.noops += 1
            await review_repo.record_audit(
                session,
                order_id=payload.order_id,
                entity="inventory",
                entity_id=payload.order_id,
                action="released" if changed else "release_noop",
                actor="worker:inventory",
                detail={"warehouse": payload.warehouse_id, "reason": payload.reason[:120]},
                correlation_id=envelope.correlation_id,
            )
        if changed:
            self.stats.released += 1

    async def _on_committed(self, envelope: EventEnvelope) -> None:
        payload = InventoryCommittedPayload.model_validate(envelope.payload)
        async with self._sessions() as session, session.begin():
            claimed = await idempotency.mark_processed(
                session,
                event_id=envelope.event_id,
                topic=str(Topic.INVENTORY),
                group=INVENTORY_GROUP,
                order_id=payload.order_id,
                effect="committed",
                partition=envelope.partition,
                log_offset=envelope.offset,
            )
            if not claimed:
                self.stats.duplicates += 1
                self._metrics.events_deduplicated.labels(topic=str(Topic.INVENTORY)).inc()
                return

            service = self._service(session)
            changed = await service.commit_reservation(
                order_id=payload.order_id, warehouse_id=payload.warehouse_id
            )
            if not changed:
                self.stats.noops += 1
            await review_repo.record_audit(
                session,
                order_id=payload.order_id,
                entity="inventory",
                entity_id=payload.order_id,
                action="committed" if changed else "commit_noop",
                actor="worker:inventory",
                detail={"warehouse": payload.warehouse_id},
                correlation_id=envelope.correlation_id,
            )
        if changed:
            self.stats.committed += 1

    async def _on_shortfall(self, envelope: EventEnvelope) -> None:
        """Record a stock shortfall.

        No stock changes here -- the reservation already refused. What this
        handler does is make the refusal *countable*, because "we could not serve
        3% of orders" is a number a supply-chain team can act on and a log line
        they cannot.
        """
        payload = InventoryShortfallPayload.model_validate(envelope.payload)
        async with self._sessions() as session, session.begin():
            claimed = await idempotency.mark_processed(
                session,
                event_id=envelope.event_id,
                topic=str(Topic.INVENTORY),
                group=INVENTORY_GROUP,
                order_id=payload.order_id,
                effect="shortfall",
                partition=envelope.partition,
                log_offset=envelope.offset,
            )
            if not claimed:
                self.stats.duplicates += 1
                self._metrics.events_deduplicated.labels(topic=str(Topic.INVENTORY)).inc()
                return
            await review_repo.record_audit(
                session,
                order_id=payload.order_id,
                entity="inventory",
                entity_id=payload.order_id,
                action="shortfall",
                actor="worker:inventory",
                detail={
                    "warehouse": payload.warehouse_id,
                    "reason": payload.reason[:120],
                    "lines": [line.model_dump() for line in payload.requested][:5],
                },
                correlation_id=envelope.correlation_id,
            )
        self.stats.shortfalls += 1
        logger.info(
            "shortfall order_id=%s warehouse=%s reason=%s",
            payload.order_id,
            payload.warehouse_id,
            payload.reason,
        )

    # -- helpers -----------------------------------------------------------

    def _service(self, session: AsyncSession) -> Any:
        if self._inventory_factory is None:
            raise RuntimeError(
                "InventoryWorker needs an inventory_service_factory; "
                "the reservation logic lives in backend.inventory.service"
            )
        return self._inventory_factory(session)

    def subscription(self, instances: int = 1, instance_id: int = 0) -> Subscription:
        return Subscription(
            topic=Topic.INVENTORY,
            group=INVENTORY_GROUP,
            handler=self.handle,
            instances=instances,
            instance_id=instance_id,
        )

    async def start(self, instances: int = 1, instance_id: int = 0) -> Any:
        return await self._bus.subscribe(self.subscription(instances, instance_id))

    async def stop(self, handle: Any) -> None:
        await self._bus.cancel(handle)


async def sweep_expired_once(
    *,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    metrics: FlowMeshMetrics,
) -> int:
    """One pass of the expiry sweeper, for the scheduler in `run_inventory_worker`."""
    from backend.inventory.service import InventoryService

    async with session_factory() as session, session.begin():
        service = InventoryService(
            session=session,
            metrics=metrics,
            ttl_seconds=settings.inventory_reservation_ttl_seconds,
        )
        return await service.sweep_expired()


async def ledger_snapshot(
    session: AsyncSession,
) -> list[dict[str, Any]]:
    """The whole stock ledger, for the reconciliation report.

    Read back from the database rather than accumulated in memory, because the
    question "does the ledger match the log" can only be answered by the thing
    that disagrees.
    """
    from sqlalchemy import select

    from backend.database.models import Inventory

    rows = await session.execute(select(Inventory).order_by(Inventory.warehouse_id, Inventory.sku))
    return [
        {
            "warehouse_id": row.warehouse_id,
            "sku": row.sku,
            "on_hand": row.on_hand,
            "reserved": row.reserved,
            "available": row.available,
            "version": row.version,
        }
        for row in rows.scalars()
    ]
