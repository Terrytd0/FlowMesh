"""Reconciliation: the log and the database must agree, by identity.

The property under test is the one that cannot be checked by counting: a
reconciliation that reports `400 applied, 400 published` is also what a pipeline
that applied two events twice and lost two others reports. So every assertion here
compares **sets of ids**.

The second property is about honesty under failure: a reconciliation that cannot
read the log must *say so and fail*, not report `missing: 0` and pass. That is the
default outcome of every reconciliation tool that only reports a count, and it is
the specific bug this module was written to avoid -- the first version read no log
at all for every transport and would have passed on a database with total data
loss.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.exc import IntegrityError

from backend.events.schema import EventEnvelope, EventType, Topic
from backend.scripts.reconcile import (
    EXPECTED_UNLEDGERED,
    ReconciliationReport,
    reconcile,
    render_markdown,
    verify,
)


class _Log:
    """A `ReadableLog` wrapping the in-process log, with a "reading raises" switch.

    Wraps rather than replaces: `publish_through` needs the real bus to publish to
    and the real consumer to drain it, while reconciliation needs something whose
    `replay()` it can be made to fail. An earlier version of this helper returned a
    *new* `_Log` holding an empty list, so the test published to one object and
    reconciled against another -- and found nothing wrong, having compared nothing.
    """

    def __init__(self, bus: Any = None) -> None:
        self._bus = bus
        self.raise_on_read = False
        self.reads = 0

    @property
    def bus(self) -> Any:
        assert self._bus is not None
        return self._bus

    async def replay(self, topic: Topic, from_offset: int = 0) -> list[EventEnvelope]:
        self.reads += 1
        if self.raise_on_read:
            raise ConnectionError("broker unreachable")
        assert topic == Topic.ORDER, "reconciliation only reads the order topic"
        return await self.bus.replay(topic, from_offset=from_offset)


def order_event() -> EventEnvelope:
    return EventEnvelope(event_type=EventType.ORDER_ACCEPTED, order_id="CRG-1")


async def publish_through(
    *, log: Any, session_factory: Any, count: int, drain: bool = True
) -> None:
    """Publish `count` orders and let the real processor consume them.

    Uses the production handler, so the ledger rows the reconciliation reads were
    written by the same code that runs in production rather than by the test.
    """
    from backend.core.ids import new_order_id
    from backend.events.schema import (
        LineItem,
        OrderAcceptedPayload,
        PaymentDetails,
        envelope_for,
    )
    from backend.pipeline.context import PipelineContext, build_order_processor
    from backend.pipeline.order_processor import ORDER_PROCESSOR_GROUP
    from backend.queues.memory_queue import InMemoryTaskQueue

    bus = log.bus
    queue = InMemoryTaskQueue(metrics=bus._metrics)
    await queue.start()
    context = PipelineContext(
        settings=_settings(),
        metrics=bus._metrics,
        event_bus=bus,
        task_queue=queue,
        scorer=bus._scorer,
        session_factory=session_factory,
    )
    processor = build_order_processor(context)
    handle = await processor.start()
    try:
        for _ in range(count):
            order_id = new_order_id()
            payload = OrderAcceptedPayload(
                order_id=order_id,
                customer_id="CUST-1",
                items=[LineItem(sku="SKU-A", quantity=1, unit_price_cents=100)],
                payment=PaymentDetails(
                    bin="411111",
                    last4="0000",
                    card_country="US",
                    billing_country="US",
                    shipping_country="US",
                    ip_country="US",
                ),
                total_cents=100,
                idempotency_key=f"recon-{order_id}",
            )
            await bus.publish(
                Topic.ORDER,
                order_id,
                envelope_for(EventType.ORDER_ACCEPTED, payload, order_id=order_id),
            )
        if drain:
            await bus.wait_for_drain(Topic.ORDER, ORDER_PROCESSOR_GROUP, timeout=30.0)
    finally:
        await bus.cancel(handle)
        await queue.stop()


def _settings() -> Any:
    from backend.config.settings import get_settings

    return get_settings()


# ---------------------------------------------------------------- the core case


async def test_a_clean_run_reconciles(concurrent_factory: Any, event_bus: Any) -> None:
    log = _with_scorer(event_bus)
    await publish_through(log=log, session_factory=concurrent_factory, count=12)
    report = await reconcile(log=log, session_factory=concurrent_factory)
    assert report.log_available
    assert report.missing == 0
    assert report.orphaned == 0
    assert report.orders_in_database == 12
    assert report.negative_stock_rows == 0
    assert report.passed, report.failures


async def test_reconciliation_compares_identity_not_count(
    concurrent_factory: Any, event_bus: Any
) -> None:
    """A count would pass this; a set comparison must fail it.

    Twelve events on the log, twelve rows in the ledger, but *not the same twelve*:
    one event was never applied and one was applied twice. A count-based tool
    reports success on exactly this.
    """
    from sqlalchemy import delete, insert, select

    from backend.database.models import ProcessedEvent

    log = _with_scorer(event_bus)
    await publish_through(log=log, session_factory=concurrent_factory, count=12)

    async with concurrent_factory() as session, session.begin():
        rows = list((await session.execute(select(ProcessedEvent))).scalars())
        assert len(rows) == 12
        # Make the ledger disagree with the log: drop one row, invent another.
        await session.delete(rows[0])
        await session.execute(
            insert(ProcessedEvent).values(
                event_id="ffffffffffffffffffffffffffffffff",
                topic=str(Topic.ORDER),
                consumer_group="flowmesh-order-processor-v1",
                order_id="CRG-GHOST",
                effect="accepted",
                applied_at=rows[1].applied_at,
            )
        )

    report = await reconcile(log=log, session_factory=concurrent_factory)
    assert report.applied == 12, "the ledger still holds twelve rows"
    assert report.total_on_log - report.expected_unledgered == 12, "the log holds twelve"
    assert report.missing == 1, "the unapplied event was not detected"
    assert report.orphaned == 1, "the phantom ledger row was not detected"
    assert not report.passed
    _ = delete


# ---------------------------------------------------------------- honesty


async def test_an_unreadable_log_fails_rather_than_passing(
    concurrent_factory: Any, event_bus: Any
) -> None:
    """The trap: no log, no missing events, green tick, nothing verified.

    The log is *present* and *raises* when read -- the case a broker outage produces,
    which is different from never having had one. It must come back as a failed
    report, not as an exception.

    It used to raise `ConnectionError` out of `reconcile()`, which is defensible as
    "don't hide a broken log" and is wrong for a tool whose output is a document: the
    caller in `main()` had no way to write the report explaining why the comparison did
    not run, which is the one thing an operator needs when the broker is down. The
    module docstring already specified this behaviour -- "`log_available=False` makes
    `verify()` fail with a message that says so" -- and the test asserted the
    opposite. So this now pins the documented contract, and adds that the reason
    survives into the report rather than being logged and dropped.
    """
    log = _with_scorer(event_bus)
    await publish_through(log=log, session_factory=concurrent_factory, count=5)
    log.raise_on_read = True

    report = await reconcile(log=log, session_factory=concurrent_factory)

    assert not report.log_available
    assert not report.passed, "an unreadable log produced a passing reconciliation"
    assert "could not be read" in report.unavailable_reason
    assert "broker unreachable" in report.unavailable_reason, (
        "the report does not say *why* the log was unreadable"
    )
    assert any("no readable event log" in failure for failure in report.failures)


async def test_an_unreadable_database_is_reported_not_raised(concurrent_factory: Any) -> None:
    """A database that will not answer produces a report, not a traceback.

    This is the same principle as the log above, and it was the worse of the two:
    with Postgres down, `make reconcile` died on a `socket.gaierror`, wrote no report
    file, and exited non-zero from an exception rather than from a finding. The run
    that most needs the document was the run that produced none.
    """

    def _broken_factory() -> Any:  # pragma: no cover - the failure is the behaviour
        raise ConnectionError("connection refused")

    report = await reconcile(log=None, session_factory=_broken_factory)

    assert not report.database_available
    assert not report.passed
    assert "could not be read" in report.unavailable_reason
    assert any("database" in failure.lower() for failure in report.failures)


async def test_no_log_at_all_is_a_failure(concurrent_factory: Any) -> None:
    """`make reconcile` against Kafka has no log. That is not a pass."""
    report = await reconcile(log=None, session_factory=concurrent_factory)
    assert not report.log_available
    assert not report.passed
    assert any("no readable event log" in failure for failure in report.failures)


def test_verify_reports_each_failure_independently() -> None:
    """One verifier, every check named. Fixing them one at a time is slow."""
    report = ReconciliationReport(
        log_available=True,
        missing=3,
        orphaned=2,
        negative_stock_rows=1,
        inventory_inconsistent=4,
    )
    verify(report)
    assert len(report.failures) == 4
    assert any("never applied" in failure for failure in report.failures)
    assert any("not on the log" in failure for failure in report.failures)
    assert any("negative" in failure for failure in report.failures)
    assert any("on_hand" in failure for failure in report.failures)


def test_a_clean_report_passes() -> None:
    report = ReconciliationReport(
        log_available=True, total_on_log=10, applied=10, orders_in_database=10
    )
    verify(report)
    assert report.passed
    assert report.failures == []


# ---------------------------------------------------------------- inventory


async def test_negative_stock_is_caught(concurrent_factory: Any, event_bus: Any) -> None:
    """The database refuses to *write* negative stock.

    Which means this check cannot be reached through the repositories -- the
    `available_non_negative` constraint fires first, and the first version of this
    test failed with exactly that IntegrityError. That is the constraint working
    correctly, and it is worth pinning in both directions: the write is refused
    *and*, with the constraint switched off, the reconciliation reports the row.

    `PRAGMA ignore_check_constraints` is the only way to see the second half, and
    it is scoped to the one transaction that performs the bad write.
    """
    from sqlalchemy import text

    from backend.loadtest.harness import _seed

    await _seed(concurrent_factory)

    # 1. The constraint refuses the write.
    async with concurrent_factory() as session, session.begin():
        with pytest.raises(IntegrityError, match="available_non_negative"):
            await session.execute(
                text(
                    "UPDATE inventory SET available = -5, reserved = 0 "
                    "WHERE warehouse_id = 'WH-01' AND sku = 'SKU-TSHIRT-M'"
                )
            )

    # 2. With the constraint bypassed, the reconciliation catches it -- which is
    #    the only reason that check exists, since a real database will not let the
    #    bad state through.
    async with concurrent_factory() as session, session.begin():
        await session.execute(text("PRAGMA ignore_check_constraints = ON"))
        await session.execute(
            text(
                "UPDATE inventory SET on_hand = 5, reserved = 0, available = -5 "
                "WHERE warehouse_id = 'WH-01' AND sku = 'SKU-TSHIRT-M'"
            )
        )

    report = await reconcile(log=None, session_factory=concurrent_factory)
    assert report.negative_stock_rows == 1
    assert not report.passed


async def test_an_inconsistent_ledger_row_is_caught(
    concurrent_factory: Any, event_bus: Any
) -> None:
    """`available != on_hand - reserved` with all three non-negative.

    This is the shape a read-then-write reservation produces, and it is the one a
    "did anything go negative?" check would miss -- every individual number is
    plausible. As with the previous test, the `available_consistent` constraint
    has to be bypassed to reach the state, which is the point: the reconciliation
    is the backstop for a database whose constraints have been dropped, restored
    from a dump taken before they existed, or reached through a driver that
    ignores them.
    """
    from sqlalchemy import text

    from backend.loadtest.harness import _seed

    await _seed(concurrent_factory)
    async with concurrent_factory() as session, session.begin():
        await session.execute(text("PRAGMA ignore_check_constraints = ON"))
        await session.execute(
            text(
                "UPDATE inventory SET on_hand = 100, reserved = 5, available = 90 "
                "WHERE warehouse_id = 'WH-01' AND sku = 'SKU-TSHIRT-M'"
            )
        )
    report = await reconcile(log=None, session_factory=concurrent_factory)
    assert report.negative_stock_rows == 0, "all three numbers are non-negative"
    assert report.inventory_inconsistent == 1
    assert not report.passed


# ---------------------------------------------------------------- expectations


def test_every_suppressed_event_type_has_a_reason() -> None:
    """A suppression without a reason is a suppression.

    This is the difference between "we know `order.scored` is not ledgered" and a
    line that makes the report look better, with nobody able to tell them apart
    six months from now.
    """
    for event_type, reason in EXPECTED_UNLEDGERED.items():
        assert reason and len(reason) > 40, f"{event_type} has no real explanation"


def test_the_suppressed_types_are_ones_this_project_actually_emits() -> None:
    """A suppression for a type nothing publishes is dead weight at best."""
    emitted = {str(event) for event in EventType}
    assert set(EXPECTED_UNLEDGERED) <= emitted, (
        f"suppressing types that are never published: {set(EXPECTED_UNLEDGERED) - emitted}"
    )


async def test_scored_events_are_excluded_and_counted(
    concurrent_factory: Any, event_bus: Any
) -> None:
    """`order.scored` is on the log and correctly absent from the ledger.

    The order processor publishes it onto the order topic and its own consumer
    group ignores it, so it must be excluded *and counted* -- an exclusion that
    does not report its size is indistinguishable from an exclusion that is
    hiding losses.
    """
    log = _with_scorer(event_bus)
    await publish_through(log=log, session_factory=concurrent_factory, count=6)
    report = await reconcile(log=log, session_factory=concurrent_factory)
    assert report.expected_unledgered == 6, "one order.scored per order, counted"
    assert report.missing == 0


# ---------------------------------------------------------------- the report


def test_the_report_states_that_an_unreadable_log_was_not_checked() -> None:
    rendered = render_markdown(ReconciliationReport(log_available=False))
    # Collapse the line wrapping: the report is a markdown document, so a phrase
    # can straddle a line break, and an assertion that does not account for that
    # fails on reformatting rather than on a change in meaning.
    flat = " ".join(rendered.split())
    assert "NO -- comparison skipped" in flat
    assert "proved nothing at all" in flat
    assert "kcat" in flat


def test_the_report_names_the_suppressed_types() -> None:
    rendered = render_markdown(ReconciliationReport(log_available=True))
    for event_type in EXPECTED_UNLEDGERED:
        assert event_type in rendered
    assert "suppressing real losses" in rendered


# ---------------------------------------------------------------- helpers


def _with_scorer(event_bus: Any) -> Any:
    """Attach a scorer and a metrics registry to the log fixture.

    `build_pipeline` normally does this; here it is set directly so the test can
    use the `concurrent_factory` fixture's connection pool, which is the one with
    real concurrency.
    """
    from prometheus_client import CollectorRegistry

    from backend.config.settings import get_settings
    from backend.fraud.engine import FraudScoringEngine
    from backend.grpc_service.client import InProcessFraudClient
    from backend.observability.metrics import FlowMeshMetrics

    metrics = FlowMeshMetrics(registry=CollectorRegistry())
    engine = FraudScoringEngine(get_settings(), metrics)
    event_bus._metrics = metrics  # type: ignore[attr-defined]
    event_bus._scorer = InProcessFraudClient(engine, metrics=metrics)  # type: ignore[attr-defined]
    return _Log(event_bus)
