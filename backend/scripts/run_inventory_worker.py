"""Run the inventory worker, plus the reservation expiry sweeper.

The sweeper runs in this process rather than as a separate service, and the
reason is worth stating: it is one `SELECT` every thirty seconds against a table
with an index on `(state, expires_at)`. A dedicated service for that is
infrastructure to operate for no benefit. It is on an interval rather than in the
consume loop so that a slow sweep cannot stall inventory event processing, and
those are genuinely different failure modes -- one delays a reclaim, the other
delays a reservation.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from backend.config.settings import get_settings
from backend.core.logging import configure_logging, get_logger
from backend.database.session import get_session_factory
from backend.events.factory import create_event_bus
from backend.events.schema import Topic
from backend.observability.metrics import get_metrics
from backend.observability.serve import serve_metrics
from backend.pipeline.context import PipelineContext, build_inventory_worker
from backend.pipeline.inventory_worker import INVENTORY_GROUP

logger = get_logger(__name__)

SWEEP_INTERVAL_SECONDS = 30.0


async def run(
    *,
    instances: int = 1,
    instance_id: int = 0,
    sweep: bool = True,
    transport_override: str | None = None,
) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_level_json)
    if transport_override is not None:
        settings.fraud_transport = transport_override

    metrics = get_metrics()
    # Serve /metrics: a worker with no web framework would otherwise export a
    # counter nobody scrapes. See backend/observability/serve.py.
    metrics_server = await serve_metrics(metrics, port=settings.metrics_port)
    bus = await create_event_bus(settings, metrics)
    session_factory = get_session_factory(settings)

    # The queue is not used by this worker, but `PipelineContext` requires one and
    # an unused dependency that is honestly `None` is better than a fake queue
    # that silently swallows publishes if someone wires it up by mistake.
    from backend.queues.memory_queue import InMemoryTaskQueue

    context = PipelineContext(
        settings=settings,
        metrics=metrics,
        event_bus=bus,
        task_queue=InMemoryTaskQueue(metrics=metrics),
        scorer=None,
        session_factory=session_factory,
    )
    worker = build_inventory_worker(context)
    await bus.ensure_topics(settings.kafka_partitions)
    handle = await worker.start(instances=instances, instance_id=instance_id)

    sweeper = (
        asyncio.create_task(_sweep_forever(session_factory, metrics, settings)) if sweep else None
    )
    logger.info(
        "inventory worker running: group=%s topic=%s instance=%s/%s partitions=%s sweeper=%s",
        INVENTORY_GROUP,
        Topic.INVENTORY,
        instance_id,
        instances,
        handle.assigned_partitions,
        "on" if sweep else "off",
    )

    try:
        await handle.task
    except asyncio.CancelledError:
        pass
    finally:
        if sweeper is not None:
            sweeper.cancel()
        await worker.stop(handle)
        await bus.stop()
        metrics_server.close()
        await metrics_server.wait_closed()
        logger.info(
            "inventory worker stopped: reserved=%s released=%s committed=%s "
            "shortfalls=%s duplicates=%s noops=%s",
            worker.stats.reserved,
            worker.stats.released,
            worker.stats.committed,
            worker.stats.shortfalls,
            worker.stats.duplicates,
            worker.stats.noops,
        )


async def _sweep_forever(session_factory: Any, metrics: Any, settings: Any) -> None:
    """Reclaim expired reservations on an interval.

    Failures are logged and the loop continues. A sweeper that exits on the first
    error stops reclaiming stock, and the symptom -- a catalogue quietly
    underselling -- appears nowhere except as unsold inventory.

    `settings` is a parameter rather than a module-level read, so this loop and
    the `InventoryService` it drives cannot be configured differently. The first
    version omitted the argument at the call site, which mypy caught and which
    would have been a `TypeError` on the first sweep in production.
    """
    from backend.pipeline.inventory_worker import sweep_expired_once

    while True:
        try:
            reclaimed = await sweep_expired_once(
                settings=settings, session_factory=session_factory, metrics=metrics
            )
            if reclaimed:
                logger.info("expiry sweeper reclaimed %s reservations", reclaimed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the sweeper must survive
            logger.exception("expiry sweep failed: %s", exc)
        try:
            await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the FlowMesh inventory worker.")
    parser.add_argument("--instances", type=int, default=1)
    parser.add_argument("--instance-id", type=int, default=0)
    parser.add_argument(
        "--no-sweep",
        action="store_true",
        help="do not run the reservation expiry sweeper in this process",
    )
    args = parser.parse_args()
    asyncio.run(
        run(instances=args.instances, instance_id=args.instance_id, sweep=not args.no_sweep)
    )


if __name__ == "__main__":
    main()
