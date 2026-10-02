"""Run the whole pipeline in one process, against SQLite and the in-process transports.

`make all-local` points here. It exists because the alternative for a first run is
five terminals, and because the *shape* of the system is easier to see in one
place: one process where an order goes in at `POST /orders` and comes out the other
end as a reservation or a review task.

**The shared-transport rule.** Every long-lived object -- the event bus, the task
queue, the session factory, the metrics registry, the engine -- is built exactly
once, by the API's lifespan, and every worker is then bound to *those* objects via
`app.state`. This is the whole reason the module is not five `run_*` scripts
launched as tasks: each of those builds its own `InMemoryEventLog` and its own
`InMemoryTaskQueue`, so the order processor would publish into one log and the
inventory worker would consume from another, and the stack would appear to run while
processing nothing. Two loops' worth of in-process transport is not "in-process".

A reader who wants the real topology should use `make up`: real Kafka, real
RabbitMQ, real Postgres, each stage in its own container. This is the single-process
development mode, and the report files state plainly which topology produced their
numbers.

`--grpc-fraud` is the one middle ground worth having: it runs the scoring boundary
as a **real** gRPC server on a real socket, in the same process, so the protobuf
conversion, the status-code mapping and the deadline behaviour are all exercised
while the brokers stay substituted. It is the closest thing to production this mode
gets, and it is off by default only because the in-process scorer is what the load
test measures.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress
from typing import Any

from backend.config.settings import get_settings
from backend.core.clock import monotonic
from backend.core.logging import configure_logging, get_logger
from backend.pipeline.context import (
    PipelineContext,
    build_inventory_worker,
    build_order_processor,
    build_review_worker,
)
from backend.pipeline.order_processor import ORDER_PROCESSOR_GROUP
from backend.queues.memory_queue import json_task

logger = get_logger(__name__)

#: Mirrors `run_review_worker.DEFAULT_CONCURRENCY`. Kept as a module constant rather
#: than imported at the top so the two modules have no import edge: the review
#: worker's own `run()` is a separate entry point, not something this one builds on.
REVIEW_CONCURRENCY = 8

PROGRESS_INTERVAL_SECONDS = 30.0

#: How long to wait for the API's lifespan before giving up. Generous, because it
#: includes engine creation, and bounded, because an unbounded wait turns a startup
#: failure into a process that looks alive and serves nothing.
API_READY_TIMEOUT_SECONDS = 60.0


class _Stack:
    """The running stages, so shutdown is a list rather than a pile of `finally`s.

    `tasks` holds the auxiliary coroutines -- progress, the review consumer, the
    notification sweeper. The API's serve task is deliberately *not* in it: that one
    is asked to stop rather than cancelled, because cancelling it means uvicorn is
    killed mid-shutdown and prints its own traceback on the way out.
    """

    def __init__(self) -> None:
        self.tasks: list[asyncio.Task[Any]] = []
        self.serve_task: asyncio.Task[Any] | None = None
        self.handles: list[Any] = []
        self.bus: Any = None
        self.queue: Any = None
        self.scorer: Any = None
        self.fraud_server: Any = None
        self.fraud_metrics_server: Any = None
        self.server: Any = None

    async def close(self) -> None:
        """Stop in dependency order, and never let teardown raise.

        The order is the part that matters:

        1. the consumers stop first, because they are reading from the transports
           that steps 3 and 4 close -- a consumer outliving its event bus fails on
           its next poll, and that failure arrives during shutdown where it is
           indistinguishable from a real one;
        2. the API is then asked to stop, gracefully. Its lifespan owns the shared
           bus, queue, scorer and engine and closes them itself; cancelling the serve
           task instead would cut that short and leave uvicorn reporting a
           `CancelledError` as though the stack had crashed;
        3. the fraud gRPC server and its metrics port go last, since the API's
           scorer may still be holding a channel to it.

        Each step is independent and a failure in one is logged rather than raised.
        Teardown that raises replaces the reason the stack was shutting down, which
        is the same mistake the load-test harness documents at length.
        """
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 - see docstring
                logger.warning("a stage did not stop cleanly: %s", exc)

        for handle in self.handles:
            try:
                await self.bus.cancel(handle)
            except Exception as exc:  # noqa: BLE001
                logger.warning("could not cancel a consumer: %s", exc)

        if self.server is not None:
            self.server.should_exit = True
        if self.serve_task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self.serve_task), timeout=30.0)
            except (TimeoutError, asyncio.CancelledError):
                self.serve_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await self.serve_task
            except Exception as exc:  # noqa: BLE001
                logger.warning("the API did not shut down cleanly: %s", exc)

        if self.fraud_server is not None:
            self.fraud_server.stop(0)
        if self.fraud_metrics_server is not None:
            self.fraud_metrics_server.close()

        # The API's lifespan already disposed the engines on its way out. This is a
        # safety net for the paths where it did not -- a failure during startup, or
        # a stage that raised before the lifespan was ever entered.
        from backend.database.session import dispose_engines

        await dispose_engines()


async def run(
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    grpc_fraud: bool = False,
    inventory_instances: int = 1,
) -> None:
    """Serve the API and run every pipeline stage until interrupted."""
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_level_json)

    if settings.storage_kind != "sqlite":
        logger.warning(
            "dev_stack is the single-process mode and expects SQLite; the settings "
            "point at %s. Use `make up` for a real topology.",
            settings.storage_kind,
        )

    stack = _Stack()
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    try:
        if grpc_fraud:
            await _start_fraud_grpc(stack, settings)

        await _start_api(stack, host=host, port=port)
        await _start_consumers(stack, inventory_instances=inventory_instances)

        logger.info(
            "dev stack ready: api=http://%s:%s/docs metrics=%s fraud=%s "
            "order_processor=%s inventory_workers=%s",
            host,
            port,
            settings.metrics_port,
            "grpc" if grpc_fraud else "in_process",
            ORDER_PROCESSOR_GROUP,
            inventory_instances,
        )
        logger.info("press Ctrl-C to stop every stage")

        await stop.wait()
    finally:
        logger.info("stopping the dev stack")
        await stack.close()


async def _start_fraud_grpc(stack: _Stack, settings: Any) -> None:
    """Start the scoring boundary as a real gRPC server on a real socket.

    Started before the API so the API's `build_fraud_backend` finds a channel that
    is already listening; a client that dials before the server binds would fail its
    first call and hold every order for review, which looks like a fraud problem and
    is a startup race.
    """
    from backend.fraud.engine import FraudScoringEngine
    from backend.grpc_service.server import serve
    from backend.observability.metrics import get_metrics
    from backend.observability.serve import serve_metrics

    metrics = get_metrics()
    engine = FraudScoringEngine(settings, metrics)
    stack.fraud_metrics_server = await serve_metrics(metrics, port=settings.fraud_metrics_port)
    stack.fraud_server, _bound = await serve(settings, engine)
    settings.fraud_transport = "grpc"
    logger.info(
        "fraud scoring served over gRPC on %s:%s (model=%s degraded=%s)",
        settings.fraud_grpc_host,
        settings.fraud_grpc_port,
        engine.model.version,
        engine.degraded,
    )


async def _start_api(stack: _Stack, *, host: str, port: int) -> None:
    """Serve the FastAPI app, then wait for its lifespan to populate `app.state`.

    The wait is the load-bearing part. The API's lifespan is where the event bus,
    the queue, the scorer and the session factory are built, and a worker bound to
    `app.state` before that is done gets `AttributeError` -- or worse, a second set
    of transports that happens to be the ones the worker writes to.

    Readiness is `app.state` being populated, *not* `uvicorn.Server.started`. That
    flag is the obvious thing to poll and it is not dependable: this was written
    against `Server.started`, which the server does set at the end of `startup()`,
    and the first version hung forever on a server that was demonstrably serving
    (`/healthz` answered) with the flag still `False`. Polling the application's own
    state is also the more honest test, since that is the thing the consumers
    actually need, and it does not change meaning when uvicorn reorganises its
    startup sequence.
    """
    import uvicorn

    from backend.main import create_app

    app = create_app()
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    stack.server = uvicorn.Server(config)
    serve_task = asyncio.create_task(stack.server.serve(), name="api")
    stack.serve_task = serve_task

    required = ("event_bus", "task_queue", "scorer", "session_factory", "metrics")
    deadline = monotonic() + API_READY_TIMEOUT_SECONDS
    while not all(hasattr(app.state, name) for name in required):
        if serve_task.done():
            # Re-raise the startup failure rather than reporting a timeout: a port
            # already in use and a missing lifespan are very different problems and
            # "the API did not become ready" tells an operator neither.
            await serve_task
            raise RuntimeError(
                "the API stopped serving before its lifespan finished; see the log above"
            )
        if monotonic() > deadline:
            raise TimeoutError(
                f"the API did not finish its lifespan within {API_READY_TIMEOUT_SECONDS:.0f}s; "
                f"app.state has {sorted(vars(app.state))}"
            )
        await asyncio.sleep(0.02)

    stack.bus = app.state.event_bus
    stack.queue = app.state.task_queue
    stack.scorer = app.state.scorer


async def _start_consumers(stack: _Stack, *, inventory_instances: int) -> None:
    """Bind the three consumer stages to the API's already-built objects."""
    from backend.pipeline.inventory_worker import INVENTORY_GROUP
    from backend.scripts.run_review_worker import NOTIFICATION_SWEEP_SECONDS

    settings = get_settings()
    app_state = stack.server.config.app.state
    metrics = app_state.metrics
    session_factory = app_state.session_factory

    context = PipelineContext(
        settings=settings,
        metrics=metrics,
        event_bus=stack.bus,
        task_queue=stack.queue,
        scorer=stack.scorer,
        session_factory=session_factory,
    )

    processor = build_order_processor(context)
    stack.handles.append(await processor.start())

    inventory = build_inventory_worker(context)
    for instance_id in range(inventory_instances):
        stack.handles.append(
            await inventory.start(instances=inventory_instances, instance_id=instance_id)
        )

    # The review stage is a *queue* consumer, not an event-log one -- that is the
    # whole argument for running two brokers (ADR-004): `review.requested` is work
    # still to be done, not a fact to be replayed. So it is started against the
    # shared `task_queue` rather than the bus, and it has no `start()`/`stop()`
    # pair -- `queue.consume` is the subscription.
    review = build_review_worker(context)
    notifier = build_review_worker(context, actor="worker:notification")
    stack.tasks.append(
        asyncio.create_task(
            stack.queue.consume(
                settings.review_queue, review.handle, concurrency=REVIEW_CONCURRENCY
            ),
            name="review-consumer",
        )
    )
    stack.tasks.append(
        asyncio.create_task(
            _notification_loop(notifier, interval=NOTIFICATION_SWEEP_SECONDS),
            name="notification-sweeper",
        )
    )

    stack.tasks.append(asyncio.create_task(_report_progress(processor), name="progress"))

    await stack.bus.ensure_topics(settings.kafka_partitions)
    logger.info(
        "stages running: order_processor(%s) inventory(%s x%s) review(queue=%s)",
        ORDER_PROCESSOR_GROUP,
        INVENTORY_GROUP,
        inventory_instances,
        settings.review_queue,
    )


async def _notification_loop(worker: Any, *, interval: float) -> None:
    """Publish notification tasks for unsent outbox rows, on an interval.

    The outbox row is written in the same transaction as the decision, so a row with
    `sent_at IS NULL` is a decision that committed whose notification has not gone
    out. This loop is what closes that gap, and its interval is the worst-case delay
    between a customer's order being approved and them being told.
    """
    while True:
        try:
            await worker.deliver_notifications(json_task({"limit": 50}))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a failed sweep must not kill the stage
            logger.exception("notification sweep failed")
        await asyncio.sleep(interval)


async def _report_progress(processor: Any) -> None:
    """Log the consumer counters periodically until cancelled.

    Not decoration. A consumer that has consumed nothing for ten minutes and one
    that is wedged look identical on a dashboard; the difference is visible in the
    handled count staying at zero, which is only readable if it is logged somewhere.
    """
    while True:
        await asyncio.sleep(PROGRESS_INTERVAL_SECONDS)
        stats = processor.stats
        logger.info(
            "dev stack progress: handled=%s approved=%s held=%s duplicates=%s shortfalls=%s",
            stats.handled,
            stats.approved,
            stats.held,
            stats.duplicates,
            len(processor.shortfalls),
        )


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """SIGINT/SIGTERM set the stop event, matching the `run_*` scripts.

    A hard kill would leave a consumer mid-handler. That is survivable -- the offset
    is uncommitted -- but it means every shutdown exercises the crash path, and a
    shutdown that logs nothing leaves no record of what was in flight.

    Not installed on Windows, where `add_signal_handler` raises
    `NotImplementedError`; there, Ctrl-C raises `KeyboardInterrupt` and the
    `finally` in `run()` does the same job.
    """
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            logger.debug("cannot install a handler for %s on this platform", name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the whole FlowMesh pipeline in one process.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--grpc-fraud",
        action="store_true",
        help="serve fraud scoring over a real gRPC socket instead of in-process",
    )
    parser.add_argument(
        "--inventory-instances",
        type=int,
        default=1,
        help="how many members of the inventory consumer group this process represents",
    )
    args = parser.parse_args()
    asyncio.run(
        run(
            host=args.host,
            port=args.port,
            grpc_fraud=args.grpc_fraud,
            inventory_instances=args.inventory_instances,
        )
    )


if __name__ == "__main__":
    main()
