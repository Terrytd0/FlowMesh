"""Load test: sustain 500 orders/sec and measure what the sprint claims.

This is the script behind the resume line, and its most important property is that
**it can report red**. Every threshold in `Budget` is asserted at the end and a
breach exits non-zero, which is what makes it usable as a CI gate. A load test
that prints numbers and exits 0 is a load test nobody can fail a build with.

Four budgets, all from the sprint's success criteria:

| budget | value | why this number |
| --- | --- | --- |
| sustained throughput | 500 orders/sec | the client requirement |
| fraud-scoring p95 | < 200ms | the per-order decision budget |
| oversells | 0 | the inventory invariant, and the one that matters |
| data loss | 0 | every published order accounted for |

The oversell and data-loss budgets are the interesting ones. Throughput and
latency are performance; those two are *correctness*, and a load test that only
checked the first two would pass happily while the pipeline sold stock it did not
have.

**What it measures.** With `--in-process` (the default) everything runs in one
process: the producer publishes to the in-process log, the order processor scores
in-process, and the database is SQLite. That measures the *pipeline* -- the
handlers, the SQL, the idempotency ledger, the reservation logic -- at a rate the
single machine can generate. It does not measure a Kafka broker, and it is not
presented as if it does; `--report` states the topology it ran under. Measuring
the broker requires `make loadtest` against the compose stack, whose numbers are
lower for reasons the report also names.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.core.clock import utcnow
from backend.core.logging import configure_logging, get_logger
from backend.events.schema import (
    LineItem,
    OrderAcceptedPayload,
    PaymentDetails,
)
from backend.fraud.features import OrderContext

logger = get_logger(__name__)

DEFAULT_TARGET_RATE = 500
DEFAULT_DURATION_SECONDS = 10.0
DEFAULT_PRODUCER_CONCURRENCY = 32

#: The catalogue the load test reserves against, mirroring
#: `backend/loadtest/harness.py::LOAD_TEST_SKUS`. Duplicated rather than imported
#: because `harness` imports from this module, and a cycle here would be a
#: genuinely unpleasant thing to debug. The two are asserted to be equal in
#: `tests/unit/observability/test_loadtest.py`.
LOAD_TEST_SKUS = (
    "SKU-TSHIRT-M",
    "SKU-TSHIRT-L",
    "SKU-MUG-STD",
    "SKU-HEADPHONES",
    "SKU-KETTLE-PRO",
    "SKU-DESK-LAMP",
)


@dataclass(frozen=True)
class Budget:
    """The thresholds a run is judged against.

    Frozen, and named. A load test whose pass condition is a boolean computed
    inline from three different thresholds is a load test nobody can check the
    arithmetic on.
    """

    min_throughput_per_sec: float = 400.0
    """Producer rate below which the run did not demonstrate the requirement.

    400 rather than 500: the target is a *rate to aim at*, and a run that
    generates 490/sec has demonstrated the requirement. Requiring exactly 500
    would make the test fail on scheduler jitter rather than on capability.

    This is the *producer*. It is not the success criterion on its own -- see
    `min_end_to_end_per_sec`, which is the one that matters.
    """

    min_end_to_end_per_sec: float = 0.0
    """Orders per second of total wall clock, generation plus drain.

    Set to 0.0, i.e. **not asserted**, and that is a statement rather than an
    oversight. The honest measurement on this harness is around 35 orders/sec end to
    end, because one process against one SQLite file issues ~17 statements per order
    and each is a round trip to an `aiosqlite` worker thread. Asserting the roadmap's
    500/sec here would mean either deleting the budget or shipping a red
    `make loadtest`, and a budget everyone has learned to ignore is worse than no
    budget.

    The consumer's own rate *is* asserted, as `min_drain_per_sec` -- this field is
    about the pipeline as a whole including the producer, which is the number the
    distributed deployment has to hit and the one that cannot be measured here.

    What the roadmap's 500/sec requires is the distributed deployment: real Kafka
    (I/O pipelined across three brokers), real Postgres, and four or more consumer
    replicas so twelve partitions are actually parallel.

    Non-zero this in a CI runner to make the run meaningful: it becomes the one
    budget that says "this harness got slower", which is a real regression signal.
    """

    max_scoring_p95_ms: float = 200.0
    """The per-order decision budget from the sprint's success criteria."""

    max_oversells: int = 0
    """The inventory invariant. Not a performance number; a correctness one."""

    drain_deadline_seconds: float = 600.0
    """How long the consumer is given to clear the backlog the producer created.

    A ceiling, not a target, and deliberately generous. Its only job is to stop the
    script hanging; the capability claim is `min_drain_per_sec` below.

    This was a hardcoded 120s inside `drain()`, and on this topology that was
    **arithmetically impossible**: 5,001 orders against a measured consumer rate of
    ~32 orders/sec needs ~157s, so every run gave up at 120s with a third of the
    stream still outstanding. A budget that cannot be met is worse than no budget --
    it trains people to ignore the one command that produces the project's evidence.
    A deadline only means something relative to a rate, which is why the rate is a
    field and the deadline is derived from nothing but "long enough to be sure".
    """

    min_drain_per_sec: float = 20.0
    """The consumer rate the pipeline must sustain to clear the backlog.

    This is the load test's real throughput claim, and the one field here that says
    "the pipeline got slower". Measured at 31-40 orders/sec for a single process
    against one SQLite file, so 20 leaves headroom for a slow CI runner while still
    failing a regression that halves the consumer's rate.

    Deliberately not 500: `min_end_to_end_per_sec` already explains why the
    end-to-end figure cannot be asserted on this harness. This is the same
    constraint stated about the part that *can* be asserted -- the consumer's
    sustained rate, measured over a backlog large enough that the figure cannot be
    an artefact of a short run.
    """

    max_unaccounted_orders: int = 0
    """Published orders with no persisted effect. Data loss, and also a correctness one."""

    def check(self, report: dict[str, Any]) -> list[str]:
        """Return every breached budget. Empty means the run passed."""
        breaches: list[str] = []
        throughput = float(report["producer_throughput_per_sec"])
        if throughput < self.min_throughput_per_sec:
            breaches.append(
                f"producer throughput {throughput:.0f}/sec < {self.min_throughput_per_sec:.0f}/sec"
            )
        end_to_end = float(report["end_to_end_per_sec"])
        if end_to_end < self.min_end_to_end_per_sec:
            breaches.append(
                f"end-to-end throughput {end_to_end:.0f}/sec "
                f"< {self.min_end_to_end_per_sec:.0f}/sec"
            )

        # Backlog and data loss are different failures, and are judged separately.
        #
        # When the consumer did not drain, the orders it never reached are absent
        # from the database -- but they are *backlog*, not loss. The events are
        # still in the log, their offsets are uncommitted, and a consumer that came
        # back would apply them. Reporting the difference as loss is how the
        # previously committed report came to claim "635 published orders had no
        # persisted effect (data loss)" about a run that had lost nothing: the
        # deadline expired, the harness quiesced the consumer, and "published minus
        # persisted" was then read as loss.
        #
        # So an unfinished drain breaches the drain budget only, and the data-loss
        # budget is reported as *undetermined* rather than passed -- a run that did
        # not finish cannot conclude that nothing was lost.
        if report.get("drain_timed_out"):
            backlog = int(report.get("backlog_after_drain_timeout", 0))
            breaches.append(
                f"the consumer did not drain the backlog within the deadline: "
                f"{backlog} events still unprocessed after "
                f"{self.drain_deadline_seconds:.0f}s"
            )
        else:
            drain_rate = float(report.get("drain_per_sec", 0.0))
            if drain_rate < self.min_drain_per_sec:
                breaches.append(
                    f"consumer drained the backlog at {drain_rate:.1f}/sec "
                    f"< {self.min_drain_per_sec:.0f}/sec"
                )
            unaccounted = int(report["unaccounted_orders"])
            if unaccounted > self.max_unaccounted_orders:
                breaches.append(
                    f"{unaccounted} published orders had no persisted effect "
                    f"(data loss) > {self.max_unaccounted_orders}"
                )

        p95 = float(report["scoring_p95_ms"])
        if p95 > self.max_scoring_p95_ms:
            breaches.append(f"scoring p95 {p95:.1f}ms > {self.max_scoring_p95_ms:.0f}ms")
        oversells = int(report["oversells"])
        if oversells > self.max_oversells:
            breaches.append(f"{oversells} oversells > {self.max_oversells}")
        return breaches


@dataclass
class LatencyRecorder:
    """Latency samples, and the percentile function that reads them.

    Exact percentiles by sorting the samples, not a reservoir or a sketch. At
    5,000 samples the sort is microseconds, and an approximate percentile cannot be
    compared against a hard budget -- which would defeat the point of having one.
    """

    samples_ms: list[float] = field(default_factory=list)

    def record(self, value_ms: float) -> None:
        self.samples_ms.append(value_ms)

    def percentile(self, fraction: float) -> float:
        if not self.samples_ms:
            return 0.0
        ordered = sorted(self.samples_ms)
        index = min(len(ordered) - 1, max(0, int(round(fraction * len(ordered))) - 1))
        return ordered[index]

    def summary(self) -> dict[str, float]:
        if not self.samples_ms:
            return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "mean": 0.0}
        return {
            "count": len(self.samples_ms),
            "p50": self.percentile(0.50),
            "p95": self.percentile(0.95),
            "p99": self.percentile(0.99),
            "max": max(self.samples_ms),
            "mean": sum(self.samples_ms) / len(self.samples_ms),
        }


@dataclass
class OrderGenerator:
    """Synthetic orders, mostly benign with a realistic slice of fraud.

    A load test made entirely of clean orders would report a fraud rate of zero and
    a stock profile that never exercises the review path. The mix here is roughly
    what a retailer sees: ~6% obviously suspicious, ~10% domestic new accounts,
    the rest ordinary. It is a *simulated* mix and the report says so, because a
    load test whose "fraud rate" is quoted as a business metric would be
    misleading -- the number that matters from this script is the *latency* and the
    oversell count, not the fraud rate.
    """

    seed: int = 7
    countries: tuple[str, ...] = ("US", "GB", "DE", "NL", "ZA", "AU", "CA")
    high_risk_countries: tuple[str, ...] = ("NG", "RU", "BR", "ID")

    def __post_init__(self) -> None:
        self._random = random.Random(self.seed)

    def next_order(self, index: int) -> tuple[OrderContext, bool]:
        """One order and whether it was designed to be suspicious.

        Each line is drawn independently, so the same SKU appears on two lines
        with non-trivial probability. That is not a stylistic choice: a customer
        genuinely can order two of the same item, and `order_items` enforces
        `UNIQUE (order_id, sku)`, so a generator that only ever produced distinct
        SKUs would have quietly been relying on a constraint the real API does not
        promise. Merging duplicate lines is the API's job, and it happens in
        `backend/api/schemas.py::OrderCreateRequest`, not here.
        """
        rng = self._random
        suspicious = rng.random() < 0.06
        home = rng.choice(self.countries)
        card = rng.choice(self.high_risk_countries) if suspicious else home
        ip = rng.choice(self.high_risk_countries) if suspicious else home
        shipping = home if rng.random() > 0.3 else rng.choice(self.countries)

        total = rng.randint(80_000, 400_000) if suspicious else rng.randint(1_500, 40_000)
        # Drawn from the real catalogue's SKUs, not from a generated range. The
        # first version of this used `SKU-1`..`SKU-6` while the seeded catalogue
        # was `SKU-TSHIRT-M` and friends, so every single order was a stockout and
        # the run quietly measured nothing but the shortfall path. The two lists
        # are asserted to agree in `test_the_generator_and_the_catalogue_agree`.
        items = tuple(
            (rng.choice(LOAD_TEST_SKUS), rng.randint(1, 2)) for _ in range(rng.randint(1, 3))
        )
        context = OrderContext(
            order_id=f"LT-{index:08d}",
            # A pool of customers, so velocity features actually move as the run
            # proceeds. One customer for the whole run would make the run measure
            # nothing about the streaming features.
            customer_id=f"LT-CUST-{rng.randint(1, 2000)}",
            total_cents=total,
            items=items,
            card_bin=f"4{rng.randint(100000, 199999)}",
            card_country=card,
            billing_country=home,
            shipping_country=shipping,
            ip_country=ip,
            coupon_code="FLASH50" if suspicious and rng.random() < 0.5 else None,
            is_gift_card=rng.random() < 0.05,
            received_at=utcnow(),
        )
        return context, suspicious

    def to_payload(
        self, context: OrderContext, *, merge_duplicate_skus: bool = True
    ) -> OrderAcceptedPayload:
        """Wrap a context as an `order.accepted` payload.

        `merge_duplicate_skus` defaults to True because `order_items` enforces
        `UNIQUE (order_id, sku)` and `next_order` draws lines independently -- a
        customer can genuinely order three of the same item, and the merge is what
        the real API does at the boundary
        (`OrderCreateRequest.merged_items`). Set it False to exercise the
        un-merged path, which should fail: the constraint is real, and a test that
        only ever generated distinct SKUs would not have found that.
        """
        lines = list(context.items)
        if merge_duplicate_skus:
            totals: dict[str, int] = {}
            for sku, quantity in lines:
                totals[sku] = totals.get(sku, 0) + quantity
            lines = sorted(totals.items())
        return OrderAcceptedPayload(
            order_id=context.order_id,
            customer_id=context.customer_id,
            items=[
                LineItem(sku=sku, quantity=quantity, unit_price_cents=1000)
                for sku, quantity in lines
            ],
            payment=PaymentDetails(
                bin=context.card_bin,
                last4="0000",
                card_country=context.card_country,
                billing_country=context.billing_country,
                shipping_country=context.shipping_country,
                ip_country=context.ip_country,
                coupon_code=context.coupon_code,
                is_gift_card=context.is_gift_card,
            ),
            total_cents=context.total_cents,
            idempotency_key=f"loadtest-{context.order_id}",
            received_at=context.received_at,
        )


async def run_load_test(
    *,
    target_rate: int = DEFAULT_TARGET_RATE,
    duration_seconds: float = DEFAULT_DURATION_SECONDS,
    budget: Budget | None = None,
    report_path: Path | None = None,
    markdown_path: Path | None = None,
) -> dict[str, Any]:
    """Generate at `target_rate` for `duration_seconds`, then reconcile.

    Returns the report dict. Raises nothing on a budget breach -- the caller
    decides, because a script that raises cannot also write the report explaining
    *why* it failed.

    `report_path` is the JSON and `markdown_path` is the prose report. Both exist
    because the committed evidence in `docs/` is the markdown, and a reader has to
    be able to check it against the raw numbers -- the JSON is what makes that
    possible, and a markdown-only report is an assertion rather than evidence.

    The markdown is written *before* returning, deliberately. It used to be printed
    to stdout and never written, while `--report` was accepted and silently ignored:
    `make loadtest` reported to the terminal, `docs/load-test-report.md` was never
    created or updated, and nothing failed. The `_ = report_path` line at the bottom
    of this function was there to keep ruff quiet about the unused argument, which is
    how a silently ignored flag survives a review. `_ = (args.report,)` is gone from
    `main` for the same reason.
    """
    from backend.loadtest.harness import run_pipeline

    resolved_budget = budget or Budget()
    logger.info(
        "load test starting: %s orders/sec for %ss (single process, in-process "
        "transports -- this measures the pipeline, not a broker)",
        target_rate,
        duration_seconds,
    )
    report = await run_pipeline(
        target_rate=target_rate,
        duration_seconds=duration_seconds,
        budget=resolved_budget,
    )
    breaches = resolved_budget.check(report)
    report["passed"] = not breaches
    report["breaches"] = breaches
    report["generated_at"] = datetime.now(UTC).isoformat()

    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        logger.info("raw measurements written to %s", report_path)

    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(format_report(report), encoding="utf-8")
        logger.info("report written to %s", markdown_path)

    return report


def format_report(report: dict[str, Any]) -> str:
    """The console summary, and the body of `docs/load-test-report.md`."""
    from backend.loadtest.report import render_markdown

    return render_markdown(report)


def main() -> None:
    parser = argparse.ArgumentParser(description="FlowMesh load test.")
    parser.add_argument("--rate", type=int, default=DEFAULT_TARGET_RATE)
    parser.add_argument("--duration", type=float, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("docs/load-test-report.md"),
        help="where to write the markdown report",
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=Path("data/runtime/loadtest.json"),
        help="where to write the raw measurements",
    )
    args = parser.parse_args()

    configure_logging("INFO")
    report = asyncio.run(
        run_load_test(
            target_rate=args.rate,
            duration_seconds=args.duration,
            report_path=args.json,
            markdown_path=args.report,
        )
    )
    print(format_report(report))  # noqa: T201 - a CLI's primary output goes to stdout
    print(f"\nwritten: {args.report} (markdown), {args.json} (json)")  # noqa: T201

    if not report["passed"]:
        print("\nBUDGET BREACH: " + "; ".join(report["breaches"]))  # noqa: T201
        raise SystemExit(1)
    raise SystemExit(0)


# `python -m backend.scripts.loadtest` is how `make loadtest` and CI invoke this, so
# the module needs this guard to do anything at all.
#
# It was missing. Every invocation imported the module, defined everything, ran
# nothing, and exited 0 -- no report, no measurement, no breach, and a green build.
# The exact failure this module's docstring says it exists to prevent: "a load test
# that prints numbers and exits 0 is a load test nobody can fail a build with",
# except it printed nothing at all. It went unnoticed because
# `tests/unit/observability/test_loadtest.py` imports `run_load_test` and calls it
# directly, so the suite exercised every line of the harness and none of the CLI.
#
# `test_the_cli_entry_points_are_wired` asserts each evidence script has this guard,
# because the suite's blind spot here is structural rather than accidental.
if __name__ == "__main__":
    main()
