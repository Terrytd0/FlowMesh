"""Run the order processor: consume `order-events`, score, route.

A separate process from the API, and deliberately so. Three reasons, in order of
how often they bite:

- **Blast radius.** An order processor that crashes or leaks memory takes the
  public endpoint with it if they share a process. Separated, a crash loop in the
  consumer leaves customers able to place orders -- they just queue up, and the
  lag gauge says so.
- **Scale independently.** Scoring concurrency and API concurrency are tuned
  against different limits. One process means one scaling decision for both.
- **Restart semantics.** A consumer holds uncommitted offsets. Restarting it
  alongside the API means every deploy replays a burst of the last few seconds.

`--instances` sets how many members of the consumer group this process
represents, so partition assignment can be exercised without six containers.

`--inline` runs a scoring pass with no gRPC hop, for a laptop with nothing else
running. The consumer code path is identical either way; only the scorer
transport differs, which is what makes the in-process mode trustworthy.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from typing import Any

from backend.config.settings import get_settings
from backend.core.logging import configure_logging, get_logger
from backend.database.session import get_session_factory
from backend.events.factory import create_event_bus
from backend.events.schema import Topic
from backend.fraud.engine import FraudScoringEngine
from backend.grpc_service.client import build_fraud_backend
from backend.observability.metrics import get_metrics
from backend.observability.serve import serve_metrics
from backend.pipeline.context import PipelineContext, build_order_processor
from backend.pipeline.order_processor import ORDER_PROCESSOR_GROUP
from backend.queues.factory import create_task_queue

logger = get_logger(__name__)


async def run(*, instances: int, instance_id: int, transport_override: str | None = None) -> None:
    """Consume until interrupted."""
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_level_json)
    if transport_override is not None:
        settings.fraud_transport = transport_override

    metrics = get_metrics()
    # Serve /metrics: a worker with no web framework would otherwise export a
    # counter nobody scrapes. See backend/observability/serve.py.
    metrics_server = await serve_metrics(metrics, port=settings.metrics_port)
    bus = await create_event_bus(settings, metrics)
    queue = await create_task_queue(settings, metrics)
    engine = FraudScoringEngine(settings, metrics)
    scorer = await build_fraud_backend(settings, engine, metrics)
    context = PipelineContext(
        settings=settings,
        metrics=metrics,
        event_bus=bus,
        task_queue=queue,
        scorer=scorer,
        session_factory=get_session_factory(settings),
    )

    processor = build_order_processor(context)
    await bus.ensure_topics(settings.kafka_partitions)
    handle = await processor.start(instances=instances, instance_id=instance_id)

    logger.info(
        "order processor running: group=%s topic=%s instance=%s/%s partitions=%s scorer=%s",
        ORDER_PROCESSOR_GROUP,
        Topic.ORDER,
        instance_id,
        instances,
        handle.assigned_partitions,
        scorer.transport,
    )

    stop = asyncio.Event()
    _install_signal_handlers(stop)

    try:
        await _run_until_stopped(processor, stop)
    finally:
        await processor.stop(handle)
        await scorer.close()
        await queue.stop()
        await bus.stop()
        metrics_server.close()
        await metrics_server.wait_closed()
        logger.info(
            "order processor stopped: handled=%s approved=%s held=%s duplicates=%s "
            "held_on_scoring_error=%s",
            processor.stats.handled,
            processor.stats.approved,
            processor.stats.held,
            processor.stats.duplicates,
            processor.stats.held_on_scoring_error,
        )


async def _run_until_stopped(processor: Any, stop: asyncio.Event) -> None:
    """Block until the stop event, logging progress as the counters move.

    The periodic log is not decoration: a consumer that has consumed nothing for
    ten minutes and a consumer that is wedged look identical on a dashboard, and
    the difference is visible here in the handled count staying at zero.
    """
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=30.0)
        except TimeoutError:
            logger.info(
                "order processor alive: handled=%s approved=%s held=%s duplicates=%s",
                processor.stats.handled,
                processor.stats.approved,
                processor.stats.held,
                processor.stats.duplicates,
            )


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """SIGINT/SIGTERM set the stop event rather than killing the process.

    A hard kill would leave the consumer mid-transaction. That is survivable --
    the offset was not committed -- but it means every deploy exercises the crash
    path, and a shutdown that logs nothing leaves no evidence of what was in
    flight.

    Not installed on Windows, where `add_signal_handler` raises
    `NotImplementedError`; there, Ctrl-C raises `KeyboardInterrupt` and the
    `finally` in `run()` does the same job.
    """
    loop = asyncio.get_running_loop()
    for signal_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, signal_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            logger.debug("cannot install a handler for %s on this platform", signal_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the FlowMesh order processor.")
    parser.add_argument(
        "--instances",
        type=int,
        default=1,
        help="total members of this consumer group, across all processes",
    )
    parser.add_argument(
        "--instance-id",
        type=int,
        default=0,
        help="this process's index within the group (0-based)",
    )
    parser.add_argument(
        "--inline",
        action="store_true",
        help="score in-process instead of over gRPC (development only)",
    )
    args = parser.parse_args()
    asyncio.run(
        run(
            instances=args.instances,
            instance_id=args.instance_id,
            transport_override="in_process" if args.inline else None,
        )
    )


if __name__ == "__main__":
    main()
