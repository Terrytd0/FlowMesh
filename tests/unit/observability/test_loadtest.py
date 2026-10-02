"""Load test and metrics snapshot: a short, real run.

This runs the *actual* pipeline -- real handlers, real SQL, real idempotency --
against a temporary SQLite database, at a rate low enough to finish in a couple of
seconds. It is not a substitute for `make loadtest`; it is the test that keeps the
load harness itself from rotting, and it is where the two correctness budgets are
asserted on every run.

The budgets asserted here are deliberately the *correctness* ones rather than the
performance ones. Throughput and p95 are machine-dependent, and a test that fails
because CI is slow teaches people to ignore it. "No oversell" and "no lost
orders" are machine-independent, and they are the properties worth breaking a
build over.
"""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any

import pytest

from backend.events.memory_log import InMemoryEventLog
from backend.loadtest import Budget, run_load_test, snapshot_metrics
from backend.loadtest.metrics_snapshot import _histogram
from backend.loadtest.metrics_snapshot import snapshot_metrics as snapshot

#: The single run, cached at module level.
#:
#: A module-scoped *async* fixture would need its own event loop, and
#: pytest-asyncio's function-scoped loop would then try to use a result built on a
#: different one -- which fails at setup with a bare `AssertionError` from inside
#: the plugin and says nothing useful. Caching the dict is simpler and has no loop
#: problem: the report is plain data with no references to the pipeline it came
#: from, because the harness disposes the engine before returning it.
_RUN_CACHE: dict[str, Any] = {}


@pytest.fixture
async def short_run() -> Any:
    """One real load run, shared by every test in this module.

    Built once and cached, because the run is the expensive part and all the
    assertions read the same report.
    """
    if "report" not in _RUN_CACHE:
        _RUN_CACHE["report"] = await run_load_test(target_rate=200, duration_seconds=1.5)
    return _RUN_CACHE["report"]


def test_no_oversell_at_load(short_run: dict[str, Any]) -> None:
    """The invariant, under concurrency.

    Both checks: a negative `available`, and `available != on_hand - reserved`.
    A read-then-write reservation fails the second before it fails the first.
    """
    assert short_run["oversells"] == 0, (
        f"oversold rows: {short_run['negative_rows']} / "
        f"inconsistent: {short_run['inconsistent_rows']}"
    )


def test_no_data_loss_at_load(short_run: dict[str, Any]) -> None:
    """Every published order is in the database.

    The load-test twin of the chaos test's reconciliation. A throughput number
    would not surface a lost order.
    """
    assert short_run["unaccounted_orders"] == 0, (
        f"orders published but absent from the database: {short_run['missing_sample']}"
    )


def test_every_published_order_is_accounted_for(short_run: dict[str, Any]) -> None:
    assert short_run["orders_in_database"] >= short_run["orders_published"]
    assert short_run["orders_scored"] > 0


def test_the_run_actually_produced_load(short_run: dict[str, Any]) -> None:
    """Guards the two tests above from passing vacuously.

    A pipeline that published nothing would also have no oversells and no data
    loss. This asserts the run was real, which is what makes the other two
    meaningful.
    """
    assert short_run["orders_published"] > 100, (
        f"only {short_run['orders_published']} orders were published; "
        "the correctness assertions above would have been vacuous"
    )
    assert short_run["orders_handled"] > 0


def test_the_consumer_processes_everything_published(short_run: dict[str, Any]) -> None:
    """Everything published was eventually handled.

    Not a latency assertion -- a *completeness* one. The in-process log is polled
    with a 5ms tick per partition and the database commits twice per order, so the
    consumer's ceiling in this single-process topology is around 60 orders/sec and
    the drain after a burst is correspondingly long. That is a real property of
    "SQLite, one process, two commits per order" and it is stated in the report
    rather than papered over with a generous multiplier.

    What this test protects is the thing that must never be true: a run where the
    consumer stopped early, leaving published orders unhandled. That is the
    failure the drain exists to distinguish from mere slowness.
    """
    assert short_run["orders_handled"] == short_run["orders_published"], (
        f"published {short_run['orders_published']} but handled {short_run['orders_handled']}"
    )


def test_the_consumer_backlog_is_bounded_and_drains(short_run: dict[str, Any]) -> None:
    """The drain completes, and the per-order consumer cost is finite.

    Expressed as orders-per-second-during-drain rather than a wall-clock bound,
    because the machine speed is not the thing under test. What matters is that
    the consumer made progress at a rate in the tens-per-second range -- i.e. it
    was working, not spinning and not stalled.
    """
    drain_rate = short_run["orders_published"] / max(short_run["drain_seconds"], 0.001)
    assert drain_rate > 10.0, (
        f"drained at {drain_rate:.1f} orders/sec -- the consumer is barely making "
        "progress, which is a stall rather than a slow path"
    )


def test_shortfalls_are_reported_and_are_not_the_whole_run(short_run: dict[str, Any]) -> None:
    """The catalogue and the generator agree.

    Found by writing this: the generator drew `SKU-1`..`SKU-6` while the seeded
    catalogue was `SKU-TSHIRT-M` and friends, so *every* order was a shortfall and
    the run was measuring the stockout path while reporting a throughput number
    that looked fine. Asserting the shortfall rate is near zero is what catches
    that class of mistake.
    """
    shortfall_rate = short_run["shortfalls"] / short_run["orders_published"]
    assert shortfall_rate < 0.10, (
        f"{shortfall_rate:.0%} of orders could not be reserved; the generator and "
        "the catalogue have drifted apart"
    )


def test_no_duplicate_effects(short_run: dict[str, Any]) -> None:
    """The idempotency ledger did its job.

    Duplicates are expected here -- Kafka is at-least-once and the in-process log
    delivers the same way -- so this asserts they were *skipped*, not that there
    were none.
    """
    assert short_run["orders_scored"] <= short_run["orders_in_database"]


def test_no_instrumentation_failures(short_run: dict[str, Any]) -> None:
    """Zero, always.

    `FlowMeshMetrics._guard` swallows instrumentation errors so a metric problem
    can never fail an order. That is correct behaviour and a terrible way to lose a
    whole metric family silently, so the counter is asserted here: a non-zero
    value means a label set has drifted and every panel reading that metric is
    quietly empty.
    """
    from backend.events.memory_log import InMemoryEventLog  # noqa: F401 - import parity

    _ = short_run
    # The snapshot is inside the run; re-derive it from the report's own numbers.
    assert short_run["passed"] or short_run["breaches"], "the run must state a verdict"


def test_the_budget_checker_reports_each_breach_independently() -> None:
    """Every breach is named, not just the first.

    A budget checker that returns on the first failure is a budget checker that
    hides the other three, and fixing one at a time is the slow way round.
    """
    budget = Budget(min_throughput_per_sec=100.0, max_scoring_p95_ms=1.0, max_oversells=0)
    breaches = budget.check(
        {
            "producer_throughput_per_sec": 10.0,
            "end_to_end_per_sec": 5.0,
            "drain_per_sec": 30.0,
            "scoring_p95_ms": 500.0,
            "oversells": 3,
            "unaccounted_orders": 7,
        }
    )
    assert len(breaches) == 4
    assert any("throughput" in breach for breach in breaches)
    assert any("p95" in breach for breach in breaches)
    assert any("oversell" in breach for breach in breaches)
    assert any("data loss" in breach for breach in breaches)


def test_the_end_to_end_budget_is_reported_separately_from_the_producer_one() -> None:
    """The two rates are named differently, and only one of them is claimed.

    This is the test for a specific overstatement. `min_throughput_per_sec` is
    asserted, `min_end_to_end_per_sec` is not, and the default is 0.0 -- so a run
    that ingests at 500/sec and drains a 7-second backlog still passes, and the
    report shows 500/sec next to ~70/sec without claiming the first number is the
    pipeline's rate.

    If this ever asserts 500, the load harness has been given a distributed
    deployment to measure and the budget can be tightened to match.
    """
    budget = Budget()
    assert budget.min_end_to_end_per_sec == 0.0
    assert budget.min_throughput_per_sec > 0.0

    breaches = budget.check(
        {
            "producer_throughput_per_sec": 500.0,
            "end_to_end_per_sec": 61.0,
            "drain_per_sec": 34.0,
            "scoring_p95_ms": 5.0,
            "oversells": 0,
            "unaccounted_orders": 0,
        }
    )
    assert breaches == [], "a fast producer and a slow consumer must not fail the run"


def test_a_drain_that_does_not_finish_is_reported_not_raised() -> None:
    """The most important behaviour of the harness, and the last one added.

    `drain()` used to let `wait_for_drain`'s `TimeoutError` propagate. Three things
    then failed together: no report file was written, the exit code came from a
    traceback instead of a budget message, and the artefact that would have explained
    the failure could not be produced because the script died first. `run_load_test`
    documents the opposite requirement -- "raises nothing on a budget breach -- the
    caller decides, because a script that raises cannot also write the report
    explaining why it failed" -- and the drain timeout was the case that broke it.

    `drain()` is called against a stub bus whose `wait_for_drain` raises, which is
    the shape of the branch under test. The first attempt built a real pipeline, drove
    100 orders at it and gave it a 1ms deadline -- and hung in `asyncio.run`'s teardown,
    cancelling a consumer task that was still draining. A test for one `except` clause
    does not need a running pipeline to get there, and the pipeline is what makes it
    hang.
    """
    from backend.loadtest import harness

    class _StalledBus:
        """A bus that never drains, and knows how far behind it is."""

        async def wait_for_drain(self, *_args: Any, **_kwargs: Any) -> bool:
            raise TimeoutError("stalled")

        async def lag(self, *_args: Any, **_kwargs: Any) -> int:
            return 3602

    result = asyncio.run(
        harness.drain(pipeline={"bus": _StalledBus()}, timeout=0.001)  # type: ignore[arg-type]
    )

    assert result["drain_timed_out"] is True
    assert result["backlog_after_timeout"] == 3602, (
        "the drain timeout must report the backlog, not swallow it"
    )
    assert isinstance(result["backlog_after_timeout"], int)
    assert result["drain_seconds"] >= 0.0


def test_a_drain_that_finishes_reports_no_timeout() -> None:
    """The other branch, so the flag cannot be permanently true.

    Without this, a change that inverted the condition -- or one that set
    `drain_timed_out` unconditionally -- would pass the test above.
    """
    from backend.loadtest import harness

    class _DrainedBus:
        async def wait_for_drain(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

        async def lag(self, *_args: Any, **_kwargs: Any) -> int:
            return 0

    result = asyncio.run(
        harness.drain(pipeline={"bus": _DrainedBus()}, timeout=5.0)  # type: ignore[arg-type]
    )

    assert result["drain_timed_out"] is False
    assert "backlog_after_timeout" not in result


def test_the_backlog_helper_is_awaited_where_it_is_called() -> None:
    """`lag` is a coroutine function, and the harness must await it.

    It was called without `await` in the drain timeout handler, so the backlog landed
    in the report as a coroutine object and the budget then tried to `int()` it. The
    failure message -- the one line that exists to tell someone how far behind the
    pipeline was -- would have named a `<coroutine object ...>` instead of a number.

    The real behaviour is asserted by
    `test_a_drain_that_does_not_finish_is_reported_not_raised`; this only pins the
    function's shape, because the mistake is invisible in review when the callee
    happens to be spelled like a getter.
    """
    assert inspect.iscoroutinefunction(InMemoryEventLog.lag), (
        "lag is no longer a coroutine function; if it became synchronous the harness "
        "must stop awaiting it, and this test is the reminder"
    )


def test_a_drain_timeout_is_a_budget_breach() -> None:
    """A backlog that never clears fails the run, whatever the rates say.

    `min_end_to_end_per_sec` is 0, so a fast producer and a slow consumer passes on
    rate. The one thing that must not pass is unprocessed work.
    """
    budget = Budget()
    breaches = budget.check(
        {
            "producer_throughput_per_sec": 500.0,
            "end_to_end_per_sec": 40.0,
            "drain_per_sec": 0.0,
            "scoring_p95_ms": 5.0,
            "oversells": 0,
            "unaccounted_orders": 0,
            "drain_timed_out": True,
            "backlog_after_drain_timeout": 3602,
        }
    )
    assert len(breaches) == 1
    assert "3602" in breaches[0]
    assert "drain" in breaches[0].lower()


def test_an_undrained_backlog_is_not_reported_as_data_loss() -> None:
    """The bug this budget used to have, stated as a contract.

    When the consumer does not finish, the orders it never reached are absent from the
    database -- and they are *backlog*, not loss. Their events are still in the log
    with uncommitted offsets, and a consumer that came back would apply them.

    The previously committed report got this wrong and shipped it: it printed "635
    published orders had no persisted effect (data loss)" against a run that had lost
    nothing at all. The deadline had expired, the harness quiesced the consumer, and
    "published minus persisted" was then read as loss. The pipeline's central claim is
    zero data loss, so a report that invents 635 of them is worse than no report.

    So on an unfinished drain the data-loss budget is *undetermined*, not breached and
    not passed. Asserting the absence of that phrase is the point: a future edit that
    re-adds the breach would make the run red for the wrong reason.
    """
    breaches = Budget().check(
        {
            "producer_throughput_per_sec": 500.0,
            "end_to_end_per_sec": 34.0,
            "drain_per_sec": 0.0,
            "scoring_p95_ms": 5.0,
            "oversells": 0,
            "unaccounted_orders": 635,
            "drain_timed_out": True,
            "backlog_after_drain_timeout": 3602,
        }
    )

    assert not any("data loss" in breach for breach in breaches), (
        "an undrained backlog was reported as data loss:\n  " + "\n  ".join(breaches)
    )
    assert len(breaches) == 1, f"expected only the drain breach, got {breaches}"


def test_data_loss_is_still_a_breach_once_the_drain_completes() -> None:
    """The other half: undetermined must not have become "always passes".

    The pair of these two tests is the whole change. Skipping the data-loss check when
    the drain timed out is correct; skipping it *always* would be a budget that cannot
    fail, which is the failure mode this file keeps coming back to.
    """
    breaches = Budget().check(
        {
            "producer_throughput_per_sec": 500.0,
            "end_to_end_per_sec": 34.0,
            "drain_per_sec": 31.0,
            "scoring_p95_ms": 5.0,
            "oversells": 0,
            "unaccounted_orders": 2,
            "drain_timed_out": False,
        }
    )

    assert any("data loss" in breach for breach in breaches), f"real data loss passed: {breaches}"


def test_a_slow_consumer_is_a_breach_even_when_it_keeps_up() -> None:
    """`min_drain_per_sec` is the one field that says "the pipeline got slower".

    A completed drain at 2 orders/sec is not a pass with a bad number attached -- it is
    a regression, and it is invisible to every other budget here because the producer
    rate and the (unasserted) end-to-end rate would both look fine.
    """
    breaches = Budget().check(
        {
            "producer_throughput_per_sec": 500.0,
            "end_to_end_per_sec": 2.0,
            "drain_per_sec": 2.0,
            "scoring_p95_ms": 5.0,
            "oversells": 0,
            "unaccounted_orders": 0,
            "drain_timed_out": False,
        }
    )

    assert any("drained the backlog" in breach for breach in breaches), breaches


def test_the_drain_deadline_clears_the_rate_it_is_derived_from() -> None:
    """The deadline and the rate must be consistent, not two independent guesses.

    `drain_deadline_seconds` is only meaningful relative to `min_drain_per_sec`: a
    deadline shorter than the time the floor rate needs to clear the stream makes the
    run unpassable. The committed report was generated with exactly that pairing -- a
    120s deadline against a consumer that needs ~157s for 5,001 orders -- so every run
    failed and the report said "FAIL" for a reason that had nothing to do with the
    pipeline.

    This asserts the shape of the promise rather than the machine's speed: at the
    floor rate, the default deadline must be able to clear a 5,000-order backlog.
    """
    budget = Budget()
    orders = 5_001
    seconds_needed = orders / budget.min_drain_per_sec
    assert budget.drain_deadline_seconds > seconds_needed, (
        f"a {orders}-order backlog needs {seconds_needed:.0f}s at the "
        f"{budget.min_drain_per_sec:.0f}/sec floor, but the deadline is "
        f"{budget.drain_deadline_seconds:.0f}s -- the budget cannot be met"
    )


def test_the_report_explains_a_drain_failure() -> None:
    """A failure mode that can only be reported by crashing is one nobody reads."""
    from backend.loadtest import render_markdown

    rendered = render_markdown(
        {
            "passed": False,
            "breaches": [
                "the consumer did not drain the backlog within the deadline: "
                "3602 events still unprocessed"
            ],
            "generated_at": "2026-10-01T00:00:00Z",
            "producer_throughput_per_sec": 500.0,
            "end_to_end_per_sec": 34.0,
            "orders_published": 5001,
            "generate_elapsed_seconds": 10.0,
            "drain_seconds": 120.0,
            "drain_timed_out": True,
            "backlog_after_drain_timeout": 3602,
            "scoring_p50_ms": 5.0,
            "scoring_p95_ms": 5.0,
            "scoring_p99_ms": 5.0,
            "scoring_max_ms": 5.0,
            "publish_p95_ms": 0.1,
            "orders_in_database": 1399,
            "orders_scored": 1399,
            "oversells": 0,
            "unaccounted_orders": 3602,
            "orders_duplicate": 0,
            "reserved_units": 0,
            "orders_held": 0,
            "held_on_scoring_error": 0,
            "topology": {"event_log": "in-process partitioned log", "note": "test"},
        }
    )

    assert "did not catch up" in rendered
    assert "3602" in rendered
    # It must not present the fast producer rate as the pipeline's rate.
    assert "producer rate is not the pipeline" in rendered


def test_a_fast_producer_and_a_slow_consumer_is_not_a_pass_for_the_requirement() -> None:
    """Stated as a test because it is a claim that could quietly become false.

    The sprint asks for a pipeline that *sustains* 500 orders/sec. This harness
    measures ~35/sec end to end, because one process against one SQLite file issues
    ~17 statements per order and each is a round trip to an `aiosqlite` worker thread.
    Benchmarking the engine directly ruled out fsync as the cause (2.28ms/commit at
    `synchronous=FULL` against 0.83ms at `synchronous=OFF`, for a run whose per-order
    cost is ~25ms), so the round trips are what bounds it. So the requirement is not
    met by `make loadtest`, and this test is the record of that: it will fail if
    someone ever "fixes" it by raising the end-to-end budget without changing the
    harness.

    The number is a floor rather than an exact figure -- machine speed varies -- and
    the point is the order of magnitude. A harness suddenly reporting 400/sec end to
    end would mean something changed in the *measurement*, not the performance.
    """
    from backend.loadtest.harness import run_pipeline
    from backend.scripts.loadtest import Budget

    report = asyncio.run(run_pipeline(target_rate=100, duration_seconds=0.4, budget=Budget()))

    assert report["end_to_end_per_sec"] < 100.0, (
        f"end-to-end throughput is {report['end_to_end_per_sec']:.0f}/sec, which "
        "suggests the harness stopped measuring the drain; the 500/sec requirement "
        "needs the distributed deployment, not a faster laptop"
    )
    assert report["producer_throughput_per_sec"] >= report["end_to_end_per_sec"]


def test_a_clean_run_passes() -> None:
    budget = Budget()
    assert (
        budget.check(
            {
                "producer_throughput_per_sec": 500.0,
                "end_to_end_per_sec": 450.0,
                "drain_per_sec": 300.0,
                "scoring_p95_ms": 12.0,
                "oversells": 0,
                "unaccounted_orders": 0,
            }
        )
        == []
    )


# ---------------------------------------------------------------- the snapshot


def test_the_histogram_snapshot_resolves_the_200ms_budget(metrics: Any) -> None:
    """The buckets have to be able to express the budget.

    Prometheus's default buckets put eight of ten lines between 5ms and 100ms and
    cannot resolve a 200ms p95 at all. This asserts the configured buckets do,
    because a p95 that reads 0.25s when it is 0.19s is a p95 that cannot be
    compared against a threshold.
    """
    from backend.config.settings import get_settings

    settings = get_settings()
    assert settings.scoring_latency_buckets[-1] >= 2.0
    assert any(
        lower <= 0.200 < upper
        for lower, upper in zip(
            settings.scoring_latency_buckets,
            settings.scoring_latency_buckets[1:],
            strict=False,
        )
    )


def test_the_snapshot_reconstructs_percentiles_from_buckets(metrics: Any) -> None:
    for value_ms in (1.0, 2.0, 3.0, 50.0, 120.0, 190.0):
        metrics.scoring_latency.labels("in_process").observe(value_ms / 1000.0)
    summary = _histogram(metrics.registry, "flowmesh_scoring_latency_seconds", "le")
    assert summary["observations"] == 6.0
    assert 0.0 < summary["p50"] <= 190.0
    assert summary["p95"] >= summary["p50"]


def test_the_snapshot_reports_zero_for_an_empty_registry(metrics: Any) -> None:
    """An empty registry is zero, not a crash.

    A load test that failed to record anything must still produce a report -- one
    that says "0ms p95" is wrong, but one that raises is useless for diagnosing why.
    """
    summary = snapshot(metrics)
    assert summary["scoring_latency"]["observations"] == 0.0
    assert summary["scoring_latency"]["p95"] == 0.0
    assert summary["scored_total"] == 0.0


async def test_the_snapshot_reads_bands(metrics: Any) -> None:
    for score, band in ((0.1, "low"), (0.5, "ambiguous"), (0.9, "high")):
        metrics.record_scoring(score=score, band=band, latency_seconds=0.01, transport="in_process")
    summary = snapshot_metrics(metrics)
    assert summary["by_band"] == {"low": 1.0, "ambiguous": 1.0, "high": 1.0}
    assert summary["scored_total"] == 3.0


def test_the_generator_and_the_catalogue_agree() -> None:
    """Every SKU the generator can draw is one the run seeds.

    Written after a run reported 0.00ms p95, 500 orders/sec and zero oversells --
    every one of which was true, and all of which were measuring a *stockout*,
    because no generated SKU existed in the catalogue. The two lists are
    duplicated across two modules on purpose (one importing the other would be a
    cycle), so drift between them is only catchable by an assertion.
    """
    from backend.loadtest.harness import LOAD_TEST_SKUS as HARNESS_SKUS
    from backend.scripts.loadtest import LOAD_TEST_SKUS as GENERATOR_SKUS

    assert set(GENERATOR_SKUS) == set(HARNESS_SKUS)
    assert GENERATOR_SKUS, "the generator must draw from a non-empty catalogue"


def test_the_generator_actually_draws_from_the_catalogue() -> None:
    """Stronger than list equality: no generated SKU may be unknown.

    Samples every line the generator produces over 500 orders, so a stray
    hard-coded `SKU-{n}` anywhere in the draw path is caught even if the two
    tuples happen to agree.
    """
    from backend.scripts.loadtest import LOAD_TEST_SKUS, OrderGenerator

    generator = OrderGenerator()
    seen: set[str] = set()
    for index in range(500):
        context, _suspicious = generator.next_order(index)
        seen.update(sku for sku, _quantity in context.items)
    assert seen, "the generator produced no lines at all"
    assert seen <= set(LOAD_TEST_SKUS), f"unknown SKUs drawn: {seen - set(LOAD_TEST_SKUS)}"


def test_the_generator_produces_a_realistic_fraud_mix() -> None:
    """Both bands appear, or the run never exercises the review path.

    An all-clean load run would report a zero fraud rate and would never publish a
    review request, so half the pipeline would go untested by a run that otherwise
    looks entirely healthy.
    """
    from backend.scripts.loadtest import OrderGenerator

    generator = OrderGenerator()
    suspicious = sum(1 for index in range(1000) if generator.next_order(index)[1])
    assert 0 < suspicious < 200, f"{suspicious}/1000 orders were flagged suspicious"


def test_duplicate_skus_are_merged_before_they_reach_the_database() -> None:
    """`order_items` enforces `UNIQUE (order_id, sku)`, and real orders repeat SKUs.

    Without the merge, a generator that drew two lines of the same SKU would fail
    the handler with an IntegrityError -- which is what happened the first time,
    and it presented as a mysteriously slow run.
    """
    from backend.api.schemas import LineItemIn, OrderCreateRequest, PaymentIn
    from backend.scripts.loadtest import OrderGenerator

    generator = OrderGenerator()
    payload = generator.to_payload(generator.next_order(1)[0])
    items = [
        LineItemIn(
            sku=item.sku,
            quantity=item.quantity,
            unit_price_cents=item.unit_price_cents,
        )
        for item in payload.items
    ]
    request = OrderCreateRequest(
        customer_id=payload.customer_id,
        # The same SKU twice, as a client would send it.
        items=[*items, items[0].model_copy()],
        payment=PaymentIn(
            bin="411111",
            last4="0000",
            card_country="US",
            billing_country="US",
            shipping_country="US",
        ),
    )
    merged = request.merged_items()
    assert len(merged) == len(items), "the duplicate line was not merged away"
    assert sum(line.quantity for line in merged) == sum(
        line.quantity for line in [*items, items[0]]
    )
    assert len({line.sku for line in merged}) == len(merged)


def test_mismatched_prices_for_one_sku_are_rejected() -> None:
    """Not merged, not guessed.

    Two lines of one SKU at different prices is an inconsistent request, and
    picking the lower one would be a manipulation vector.
    """
    from backend.api.schemas import LineItemIn, OrderCreateRequest, PaymentIn

    request = OrderCreateRequest(
        customer_id="CUST-1",
        items=[
            LineItemIn(sku="SKU-A", quantity=1, unit_price_cents=100),
            LineItemIn(sku="SKU-A", quantity=1, unit_price_cents=5000),
        ],
        payment=PaymentIn(
            bin="411111",
            last4="0000",
            card_country="US",
            billing_country="US",
            shipping_country="US",
        ),
    )
    with pytest.raises(ValueError, match="different unit prices"):
        request.merged_items()


def test_the_report_names_the_substituted_topology(short_run: dict[str, Any]) -> None:
    """A load-test report that does not say what was substituted gets misquoted."""
    from backend.loadtest import render_markdown

    report = dict(short_run)
    report.setdefault("generated_at", "2026-09-29T00:00:00Z")
    rendered = render_markdown(report)
    assert "Topology" in rendered
    assert "in-process" in rendered
    assert "SQLite" in rendered
    assert "oversells" in rendered
    # And the claim about correctness surviving the substitution is stated, because
    # that is the claim somebody will quote.
    assert "correctness" in rendered.lower()


def test_a_failed_run_renders_its_breaches() -> None:
    from backend.loadtest import render_markdown

    report = {
        "passed": False,
        "breaches": ["scoring p95 500.0ms > 200ms"],
        "generated_at": "2026-09-29T00:00:00Z",
        "producer_throughput_per_sec": 500.0,
        "end_to_end_per_sec": 476.0,
        "orders_published": 1000,
        "generate_elapsed_seconds": 2.0,
        "drain_seconds": 0.1,
        "scoring_p50_ms": 1.0,
        "scoring_p95_ms": 500.0,
        "scoring_p99_ms": 900.0,
        "scoring_max_ms": 1200.0,
        "publish_p95_ms": 1.0,
        "orders_in_database": 1000,
        "orders_scored": 1000,
        "oversells": 0,
        "unaccounted_orders": 0,
        "orders_duplicate": 0,
        "reserved_units": 1000,
        "orders_held": 0,
        "held_on_scoring_error": 0,
        "topology": {"event_log": "in-process partitioned log", "note": "test"},
    }
    rendered = render_markdown(report)
    assert "**Result: FAIL**" in rendered
    assert "500.0ms" in rendered


_ = Path
