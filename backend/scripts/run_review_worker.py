"""Run the review worker: consume `review-queue`, apply decisions.

Two consumers in one process, deliberately. The review queue and the notification
queue have different failure modes and different latencies of consequence, and
separating them into two services is an operational decision this project does not
need to make. What they *do* share is a session factory and a metrics registry,
and running them together keeps that shared setup in one place.

The concurrency setting matters more here than on the other workers. Review tasks
are short transactions against the review table, and RabbitMQ's prefetch (not
this number) is what actually bounds in-flight work -- but the number has to be
high enough that the queue drains, or the review dashboard shows a backlog that is
really just an under-provisioned consumer.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from backend.config.settings import get_settings
from backend.core.logging import configure_logging, get_logger
from backend.database.session import get_session_factory
from backend.events.factory import create_event_bus
from backend.observability.metrics import get_metrics
from backend.observability.serve import serve_metrics
from backend.pipeline.context import PipelineContext, build_review_worker
from backend.queues.factory import create_task_queue
from backend.queues.memory_queue import json_task

logger = get_logger(__name__)

DEFAULT_CONCURRENCY = 8
#: How often a sweep publishes notification tasks. The worker marks the outbox rows
#: sent; the interval is the SLA on "approved but the customer has not been told".
NOTIFICATION_SWEEP_SECONDS = 10.0


async def run(*, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_level_json)

    metrics = get_metrics()
    # Serve /metrics: a worker with no web framework would otherwise export a
    # counter nobody scrapes. See backend/observability/serve.py.
    metrics_server = await serve_metrics(metrics, port=settings.metrics_port)
    queue = await create_task_queue(settings, metrics)
    bus = await create_event_bus(settings, metrics)
    context = PipelineContext(
        settings=settings,
        metrics=metrics,
        event_bus=bus,
        task_queue=queue,
        scorer=None,
        session_factory=get_session_factory(settings),
    )
    worker = build_review_worker(context)
    notifier = build_review_worker(context, actor="worker:notification")

    consumer = asyncio.create_task(
        queue.consume(settings.review_queue, worker.handle, concurrency=concurrency),
        name="review-consumer",
    )
    sweeper = asyncio.create_task(_notification_loop(notifier), name="notification-sweeper")

    logger.info(
        "review worker running: queue=%s concurrency=%s notifier=%s",
        settings.review_queue,
        concurrency,
        settings.notification_queue,
    )

    try:
        await consumer
    except asyncio.CancelledError:
        pass
    finally:
        sweeper.cancel()
        await queue.stop()
        await bus.stop()
        metrics_server.close()
        await metrics_server.wait_closed()
        logger.info(
            "review worker stopped: processed=%s approved=%s rejected=%s "
            "already_decided=%s failed=%s notifications=%s",
            worker.stats.processed,
            worker.stats.approved,
            worker.stats.rejected,
            worker.stats.already_decided,
            worker.stats.failed,
            worker.stats.notifications,
        )


async def _notification_loop(worker: Any) -> None:
    """Publish notification tasks for unsent outbox rows, on an interval.

    The outbox is written in the same transaction as the decision, so a row with
    `sent_at IS NULL` is a decision that committed and whose notification has not
    gone out. This loop is the thing that closes that gap, and its interval is the
    worst-case delay a customer experiences between approval and email.
    """
    while True:
        try:
            await worker.deliver_notifications(json_task({"limit": 50}))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the loop must survive
            logger.exception("notification sweep failed: %s", exc)
        try:
            await asyncio.sleep(NOTIFICATION_SWEEP_SECONDS)
        except asyncio.CancelledError:
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the FlowMesh review worker.")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="concurrent task handlers (RabbitMQ's prefetch is the real bound)",
    )
    args = parser.parse_args()
    asyncio.run(run(concurrency=args.concurrency))


if __name__ == "__main__":
    main()
