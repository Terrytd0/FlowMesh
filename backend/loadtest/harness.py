"""The load-test harness: build a real pipeline, drive it, reconcile it.

Split out of `backend/scripts/loadtest.py` so the script stays a CLI and this
stays a component. It can be imported by a test, by a CI job, or by the smoke
script, which is what makes the load test *reproducible* rather than a thing
somebody runs once and quotes from memory.

The pipeline it builds is the real one, assembled from the same factories
`backend/scripts/run_*.py` use -- same processors, same repositories, same SQL,
same idempotency ledger. What differs from production is the four transports:

| production | here | why |
| --- | --- | --- |
| Kafka | in-process partitioned log | one machine cannot host a broker and be the load generator |
| RabbitMQ | in-process queue | same |
| gRPC fraud service | in-process engine | wire cost measured in the integration tests |
| PostgreSQL | SQLite | same SQL, same constraints, no server to provision |

Each substitution is a *measurement* claim, not a correctness one: the handlers
are identical, so oversell counts and data-loss counts from this harness are real.
Throughput and latency are real for this topology, and the report says which
topology it was.

Two things this measures that a naive load test does not, and they are the two
that matter:

- **Oversells**, by reconciling `sum(reserved) + sum(available) == sum(on_hand)`
  and checking no row went negative. A read-then-write reservation passes a
  throughput test and fails this.
- **Unaccounted orders**, by comparing every published `order_id` against every
  order in the database. This is the load-test twin of the chaos test's
  reconciliation.
"""

from __future__ import annotations

import asyncio
import random
from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from backend.core.clock import monotonic
from backend.core.logging import get_logger
from backend.database.base import Base
from backend.database.models import Inventory, Order
from backend.database.repositories import inventory as inventory_repo
from backend.events.memory_log import InMemoryEventLog
from backend.events.schema import EventType, Topic, envelope_for
from backend.fraud.engine import FraudScoringEngine
from backend.observability.metrics import FlowMeshMetrics
from backend.pipeline.context import PipelineContext, build_order_processor
from backend.queues.memory_queue import InMemoryTaskQueue

logger = get_logger(__name__)

#: SKUs the load test reserves against. Matches the seeded catalogue, minus the
#: deliberately-scarce one, so the run is not dominated by shortfall refusals.
#: The catalogue the run reserves against. Must match the SKUs
#: `OrderGenerator` draws from, or every order is a shortfall and the run measures
#: a stockout instead of the pipeline. Asserted by
#: `test_the_generator_and_the_catalogue_agree`.
LOAD_TEST_SKUS = (
    "SKU-TSHIRT-M",
    "SKU-TSHIRT-L",
    "SKU-MUG-STD",
    "SKU-HEADPHONES",
    "SKU-KETTLE-PRO",
    "SKU-DESK-LAMP",
)
WAREHOUSES = tuple(f"WH-{index:02d}" for index in range(1, 13))

#: Per-SKU stock for the run. High enough that a 5,000-order run does not run the
#: catalogue dry, which would make the run a stockout test instead of a load test.
#: Per-SKU stock, per warehouse. Sized from the run, not guessed: a 5,000-order run
#: at ~1.5 units per order reserves roughly 7,500 units spread across 12
#: warehouses, so 10,000 per SKU per warehouse (120,000 total) leaves the catalogue
#: untouched.
#:
#: The earlier value of 200,000 was a guess that turned out to be a *different*
#: bug: the generator draws `SKU-1`..`SKU-6` while the catalogue is
#: `SKU-TSHIRT-M`..`SKU-DESK-LAMP`, so every order was a shortfall and the run was
#: measuring a stockout. The report now counts shortfalls, and this comment records
#: why the two lists must agree.
STOCK_PER_SKU = 10_000


async def build_pipeline(*, database_path: Path, partitions: int = 12) -> dict[str, Any]:
    """A complete, self-contained pipeline over a temporary database.

    Returns the handles the driver needs. The engine is file-backed rather than
    `:memory:` for the same reason the test fixture's `concurrent_factory` is:
    `:memory:` with `StaticPool` gives every session the *same* connection, so
    500 concurrent reservations queue behind one connection and the test stops
    measuring concurrency at all.
    """
    from prometheus_client import CollectorRegistry

    from backend.config.settings import get_settings
    from backend.database.session import apply_sqlite_pragmas
    from backend.events.schema import EventEnvelope
    from backend.grpc_service.client import InProcessFraudClient
    from backend.queues.memory_queue import json_task

    settings = get_settings()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    url = "sqlite+aiosqlite:///" + database_path.as_posix()

    engine = create_async_engine(
        url,
        connect_args={"check_same_thread": False, "timeout": 60},
        pool_size=20,
        max_overflow=40,
        future=True,
    )
    # The same pragmas the application engine gets, from the same function.
    #
    # This used to be a local copy with `busy_timeout=60000` and no
    # `BEGIN IMMEDIATE`, which meant the harness exercised a *different* SQLite
    # configuration from the one `README.md`'s quick start tells you to run --
    # and a worse one. Without `BEGIN IMMEDIATE`, a transaction that reads before
    # it writes takes a SHARED lock and then cannot upgrade to RESERVED, and
    # SQLite fails that immediately rather than waiting, so `busy_timeout` never
    # came into play. That surfaced as `database is locked` on the idempotency
    # INSERT under concurrency, and as chaos-test hangs. One helper, one
    # configuration, and the load test measures the pipeline as it is deployed.
    apply_sqlite_pragmas(engine, busy_timeout_ms=60_000)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    metrics = FlowMeshMetrics(registry=CollectorRegistry())
    bus = InMemoryEventLog(metrics=metrics, partitions=partitions)
    await bus.start()
    queue = InMemoryTaskQueue(metrics=metrics)
    await queue.start()
    scorer_engine = FraudScoringEngine(settings, metrics)
    scorer = InProcessFraudClient(scorer_engine, metrics=metrics)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

    await _seed(session_factory)

    context = PipelineContext(
        settings=settings,
        metrics=metrics,
        event_bus=bus,
        task_queue=queue,
        scorer=scorer,
        session_factory=session_factory,
    )
    processor = build_order_processor(context)
    handle = await processor.start()

    _ = (EventEnvelope, json_task)
    return {
        "engine": engine,
        "bus": bus,
        "queue": queue,
        "metrics": metrics,
        "processor": processor,
        "handle": handle,
        "session_factory": session_factory,
        "scorer_engine": scorer_engine,
    }


async def _seed(session_factory: Any) -> None:
    """Warehouses and catalogue. Real rows, not a fixture, so the SQL is the same."""
    async with session_factory() as session, session.begin():
        await inventory_repo.seed_warehouses(
            session,
            [{"id": warehouse, "name": warehouse, "region": "test"} for warehouse in WAREHOUSES],
        )
        await inventory_repo.seed_inventory(
            session,
            [
                {"warehouse_id": warehouse, "sku": sku, "on_hand": STOCK_PER_SKU}
                for warehouse in WAREHOUSES
                for sku in LOAD_TEST_SKUS
            ],
        )


async def drive(
    *,
    pipeline: dict[str, Any],
    target_rate: int,
    duration_seconds: float,
    merge_duplicate_skus: bool = True,
) -> dict[str, Any]:
    """Publish orders at a fixed rate for a fixed duration.

    **Open-loop, not closed-loop.** Each producer sleeps until its next scheduled
    slot and publishes regardless of whether the consumer has kept up. A
    closed-loop generator (send, wait for a response, repeat) self-throttles to
    whatever the slowest component can do -- so it would report the pipeline's
    throughput as its *input* rate and quietly measure nothing. Open-loop is how
    you find out where the queue starts growing.

    The scheduled slots are computed from a single start timestamp, so the rate
    does not drift with accumulated per-iteration overhead.
    """
    from backend.scripts.loadtest import OrderGenerator

    bus: InMemoryEventLog = pipeline["bus"]
    generator = OrderGenerator()
    interval = 1.0 / target_rate

    published: list[str] = []
    publish_latencies: list[float] = []
    start = monotonic()
    deadline = start + duration_seconds

    # 32 concurrent publishers, each responsible for every 32nd slot. One task
    # publishing 5,000 orders would spend most of its time in its own loop rather
    # than in the broker round trip, and the measured latency would be this
    # script's.
    worker_count = 32

    async def _publish_slots(worker_index: int) -> None:
        index = worker_index
        while True:
            scheduled = start + index * interval
            if scheduled > deadline:
                return
            delay = scheduled - monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            context, _suspicious = generator.next_order(index)
            payload = generator.to_payload(context, merge_duplicate_skus=merge_duplicate_skus)
            event = envelope_for(EventType.ORDER_ACCEPTED, payload, order_id=context.order_id)
            slot_start = monotonic()
            await bus.publish(Topic.ORDER, context.order_id, event)
            publish_latencies.append((monotonic() - slot_start) * 1000)
            published.append(context.order_id)
            index += worker_count

    await asyncio.gather(*(_publish_slots(worker) for worker in range(worker_count)))
    generate_elapsed = monotonic() - start
    logger.info("published %s orders in %.2fs", len(published), generate_elapsed)

    return {
        "published": published,
        "publish_latencies": publish_latencies,
        "generate_elapsed_seconds": generate_elapsed,
    }


async def drain(*, pipeline: dict[str, Any], timeout: float = 600.0) -> dict[str, Any]:
    """Wait for the consumer to catch up, then report how long it took.

    The catch-up time *is* the backpressure measurement. A pipeline that cannot
    keep up does not fail a load test by erroring; it fails by having a consumer
    lag that grows without bound, and the only way to see that is to publish, stop,
    and time the drain.

    A drain that does not finish is **reported, not raised**. It used to let
    `wait_for_drain`'s `TimeoutError` propagate, which meant the three things that
    matter most all failed at once: no report was written, the exit code came from a
    traceback rather than a budget message, and the report that *would* have
    explained the failure could not be produced because the script died first.
    `run_load_test`'s own docstring says "raises nothing on a budget breach -- the
    caller decides, because a script that raises cannot also write the report
    explaining why it failed", and the drain timeout was exactly that case.

    Timing out is a legitimate result, not an exception, and the useful artefact is
    a report saying "N events still outstanding after T seconds" rather than a stack
    trace. `Budget` turns it into a breach, so `make loadtest` still fails.

    The default is 600s, from `Budget.drain_deadline_seconds`. It was 120s, which
    this topology cannot meet at 5,001 orders -- see that field for why a deadline
    that no run can satisfy is not a budget.
    """
    bus: InMemoryEventLog = pipeline["bus"]
    from backend.pipeline.order_processor import ORDER_PROCESSOR_GROUP

    started = monotonic()
    try:
        await bus.wait_for_drain(Topic.ORDER, ORDER_PROCESSOR_GROUP, timeout=timeout)
    except TimeoutError:
        # `await`ed, and that detail cost a debugging session. `InMemoryEventLog.lag`
        # is a coroutine function, so calling it without awaiting produced a
        # `<coroutine object InMemoryEventLog.lag at 0x...>` that was then
        # interpolated straight into the log line and into
        # `backlog_after_timeout`. The report said "0 events still outstanding" via
        # `int()`, which is a TypeError on a coroutine, and the budget's message --
        # the one thing this whole path exists to produce -- would have named a
        # coroutine instead of a backlog.
        lag = await bus.lag(Topic.ORDER, ORDER_PROCESSOR_GROUP)
        logger.error(
            "the consumer did not catch up in %.0fs; %d events are still outstanding",
            timeout,
            lag,
        )
        return {
            "drain_seconds": monotonic() - started,
            "drain_timed_out": True,
            "backlog_after_timeout": lag,
        }
    return {"drain_seconds": monotonic() - started, "drain_timed_out": False}


async def _quiesce(pipeline: dict[str, Any]) -> None:
    """Stop the consumers so a reconcile reads a database nobody is writing to.

    Best-effort and bounded: `bus.cancel` is already called again in `run_pipeline`'s
    teardown and is idempotent, and a failure here must not prevent the report from
    being written. Swallowed deliberately -- the alternative is that a teardown
    problem replaces the measurement, which is the failure mode this whole
    restructure exists to remove.
    """
    handle = pipeline.get("handle")
    bus = pipeline.get("bus")
    if bus is None or handle is None:
        return
    try:
        await asyncio.wait_for(bus.cancel(handle), timeout=30.0)
    except (TimeoutError, asyncio.CancelledError):
        logger.warning(
            "could not stop the consumer before reconciling; the counts below may be partial"
        )


async def reconcile(*, pipeline: dict[str, Any], published: list[str]) -> dict[str, Any]:
    """Check the two correctness budgets. These are what make this a load test.

    - **oversells**: any inventory row with `available < 0`, or any row where
      `available != on_hand - reserved`. Both are violations; the second is how a
      stock bug hides while both numbers look plausible.
    - **unaccounted**: every published order must exist in the database. An order
      the pipeline accepted and then lost is the worst outcome a load test can
      find, and a throughput number alone would never surface it.
    """
    # `pipeline["engine"]` first, and unconditionally.
    #
    # When the drain times out the consumer is still working through its backlog, and
    # reconciling at that moment is both meaningless and slow. Meaningless: the
    # "unaccounted" count would depend on exactly when this ran, so the same run could
    # report 0 and then 4000 depending on scheduling. Slow, concretely: SQLite allows
    # one writer, so `select(Order.id)` on 5,001 rows contends with the consumer's
    # per-order commits and each one waits out the 30s `busy_timeout`. The first
    # version of this function ran while the consumer was still draining and stalled
    # for minutes past the drain deadline -- and it would have reported a partial
    # count either way.
    #
    # So: quiesce first, then read a settled database. Orders the consumer did not
    # reach are reported as unaccounted, which is the honest reading -- they *were*
    # published and they are not in the database. The caller cancels the consumer in
    # its own teardown; all that happens here is that the reconcile no longer races it.
    await _quiesce(pipeline)

    async with pipeline["session_factory"]() as session:
        negative = await session.execute(select(Inventory).where(Inventory.available < 0).limit(10))
        negative_rows = list(negative.scalars())

        inconsistent = await session.execute(
            select(Inventory)
            .where(Inventory.available != Inventory.on_hand - Inventory.reserved)
            .limit(10)
        )
        inconsistent_rows = list(inconsistent.scalars())

        orders = await session.execute(select(Order.id))
        order_ids = {row for row in orders.scalars()}

        total_orders = await session.execute(select(func.count()).select_from(Order))
        total = int(total_orders.scalar_one())

        scored = await session.execute(
            select(func.count()).select_from(Order).where(Order.fraud_score.is_not(None))
        )
        scored_count = int(scored.scalar_one())

        reserved_total = await session.execute(
            select(func.coalesce(func.sum(Inventory.reserved), 0))
        )
        reserved = int(reserved_total.scalar_one())

    missing = [order_id for order_id in published if order_id not in order_ids]
    return {
        "orders_in_database": total,
        "orders_scored": scored_count,
        "published": len(published),
        "unaccounted_orders": len(missing),
        "missing_sample": missing[:5],
        "oversells": len(negative_rows) + len(inconsistent_rows),
        "negative_rows": [
            {"warehouse_id": row.warehouse_id, "sku": row.sku, "available": row.available}
            for row in negative_rows
        ],
        "inconsistent_rows": [
            {
                "warehouse_id": row.warehouse_id,
                "sku": row.sku,
                "available": row.available,
                "on_hand": row.on_hand,
                "reserved": row.reserved,
            }
            for row in inconsistent_rows
        ],
        "reserved_units": reserved,
    }


async def run_pipeline(*, target_rate: int, duration_seconds: float, budget: Any) -> dict[str, Any]:
    """Build, drive, drain, reconcile, and assemble the report."""
    import shutil
    import tempfile

    from backend.loadtest.metrics_snapshot import snapshot_metrics

    # A `TemporaryDirectory` context manager, but cleaned up by hand, because the
    # cleanup raised and replaced the real failure.
    #
    # On Windows, `shutil.rmtree` cannot unlink a file that any handle still holds.
    # The engine is disposed in the `finally` above, but aiosqlite closes its
    # connections on a worker thread, so at the moment `__exit__` runs the file can
    # still be locked -- and the resulting `PermissionError` propagated out of
    # `__exit__`, replacing whatever the run had actually failed with. The observed
    # symptom was a load test reporting
    # `PermissionError: [WinError 32] ... flowmesh-loadtest-*/loadtest.sqlite3`,
    # which says nothing about load, when the run had really failed with a drain
    # timeout 3602 events behind.
    #
    # Cleanup problems are logged, never raised. A leftover temporary directory is
    # cosmetic; losing the actual reason a run failed is not.
    directory = tempfile.mkdtemp(prefix="flowmesh-loadtest-")
    try:
        pipeline = await build_pipeline(database_path=Path(directory) / "loadtest.sqlite3")
        try:
            drive_result = await drive(
                pipeline=pipeline, target_rate=target_rate, duration_seconds=duration_seconds
            )
            # Sampled either side of the drain so the catch-up rate is the
            # consumer's *sustained* rate over the window it actually spent
            # catching up, rather than all published orders over all elapsed time.
            # Dividing the total by the drain alone would credit the consumer with
            # work it did during generation and inflate the figure; dividing by
            # generation+drain would fold in the producer's rate and report the
            # end-to-end number twice.
            handled_before_drain = pipeline["processor"].stats.handled
            drain_result = await drain(
                pipeline=pipeline, timeout=float(budget.drain_deadline_seconds)
            )
            handled_after_drain = pipeline["processor"].stats.handled
            recon = await reconcile(pipeline=pipeline, published=drive_result["published"])
            metrics = snapshot_metrics(pipeline["metrics"])
            processor = pipeline["processor"]
        finally:
            # Cancel first, then stop. `handle.task` is the consume loop, which
            # runs until cancelled -- awaiting it *before* cancelling it is how this
            # harness hung, and it hung silently: the run had finished, the report
            # was built, and the process sat in a bare `await` with no output.
            # Teardown order is cancel -> await the cancelled task -> stop the
            # clients -> dispose the engine.
            await pipeline["bus"].cancel(pipeline["handle"])
            await pipeline["queue"].stop()
            await pipeline["bus"].stop()
            await pipeline["engine"].dispose()
    finally:
        try:
            shutil.rmtree(directory, ignore_errors=True)
        except OSError as error:  # pragma: no cover - platform-specific
            logger.warning("could not remove the temporary database at %s: %s", directory, error)

    published = len(drive_result["published"])
    elapsed = drive_result["generate_elapsed_seconds"]
    # Two rates, because "throughput" was one word doing two jobs and the
    # requirement is about the slower of them.
    #
    # `throughput_per_sec` is the *producer*: orders generated and published per
    # second during the generation window, excluding the drain. This is a real
    # measurement of the API-and-publish path and it is what the dashboard's
    # Orders/sec panel shows.
    #
    # `end_to_end_per_sec` is orders per second of *wall clock* from the first
    # publish to the last one applied -- generation plus drain. This is the number
    # the "sustains 500 orders/sec" claim actually means, and it is the honest one:
    # a producer that publishes 500/sec while the consumer drains 7 seconds of
    # backlog afterwards has not demonstrated a 500/sec pipeline, it has
    # demonstrated a 500/sec producer and a ~70/sec consumer.
    #
    # The first version of this harness reported only the producer rate and the
    # budget checked only that, so `make loadtest` printed "500 orders/sec
    # sustained" above a consumer that could not do 100. Both numbers are now
    # reported and the budget checks the end-to-end one.
    throughput = published / elapsed if elapsed > 0 else 0.0
    drain_seconds = drain_result["drain_seconds"]
    total_seconds = elapsed + drain_seconds
    end_to_end = published / total_seconds if total_seconds > 0 else 0.0

    scoring = metrics["scoring_latency"]
    if scoring.get("empty"):
        # Better to say so than to report 0.00ms p95, which reads as "incredibly
        # fast" rather than "nothing was recorded". The caller can decide whether a
        # run that scored nothing should pass a latency budget; this script should
        # not quietly report a triumph.
        logger.error(
            "no scoring latency samples were recorded; the latency figures in the "
            "report are meaningless"
        )
    return {
        "throughput_per_sec": throughput,
        #: Producer rate only. Labelled as such because `end_to_end_per_sec` below
        #: is the figure the sprint's success criteria are about.
        "producer_throughput_per_sec": throughput,
        "end_to_end_per_sec": end_to_end,
        "orders_published": published,
        "orders_handled": processor.stats.handled,
        #: Orders the scorer approved that no warehouse could serve. Not a failure
        #: -- the pipeline handled them correctly by routing them to review -- but
        #: the count is what tells you whether the seeded catalogue was the right
        #: size, and a run where most orders shortfalls is measuring a stockout.
        "shortfalls": len(processor.shortfalls),
        "shortfall_sample": processor.shortfalls[:5],
        "orders_approved": processor.stats.approved,
        "orders_held": processor.stats.held,
        "orders_duplicate": processor.stats.duplicates,
        "held_on_scoring_error": processor.stats.held_on_scoring_error,
        "generate_elapsed_seconds": elapsed,
        "drain_seconds": drain_seconds,
        #: The consumer's sustained rate over the backlog. This is the figure
        #: `Budget.min_drain_per_sec` is checked against, and it is measured over
        #: the catch-up window only -- excluding generation -- so it is the
        #: consumer's rate and not the producer's.
        "drain_per_sec": (
            (handled_after_drain - handled_before_drain) / drain_seconds
            if drain_seconds > 0
            else 0.0
        ),
        #: True when the consumer did not finish before the drain deadline. Carried
        #: into the report and into `Budget`, because "the pipeline could not process
        #: what it accepted within two minutes" is a breach, not a slow number.
        "drain_timed_out": drain_result["drain_timed_out"],
        "backlog_after_drain_timeout": drain_result.get("backlog_after_timeout", 0),
        "scoring_p50_ms": scoring["p50"],
        "scoring_p95_ms": scoring["p95"],
        "scoring_p99_ms": scoring["p99"],
        "scoring_max_ms": scoring["max"],
        "scoring_observations": scoring["observations"],
        "scoring_is_bucket_bound": scoring.get("p95_is_upper_bound", False),
        "publish_p95_ms": _percentile(drive_result["publish_latencies"], 0.95),
        "publish_observations": len(drive_result["publish_latencies"]),
        "topology": {
            "event_log": "in-process partitioned log",
            "task_queue": "in-process queue",
            "fraud_scorer": "in-process (no gRPC hop)",
            "database": "SQLite (WAL)",
            "note": (
                "Handlers, SQL and idempotency are the production ones; the four "
                "transports are substituted. Correctness budgets from this run are "
                "real; throughput and latency are real for this topology only."
            ),
        },
        **recon,
    }


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * len(ordered))) - 1))
    return ordered[index]


_ = (random, timedelta)
