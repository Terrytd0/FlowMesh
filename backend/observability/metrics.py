"""Prometheus metrics for the whole pipeline.

One registry per process, one `FlowMeshMetrics` object wrapping every metric
this project exposes. Collected deliberately rather than scattered:

- **A metric is a contract.** `flowmesh_scoring_latency_seconds` is read by a
  Grafana panel, a CI load-test assertion and the resume line. If three
  modules each defined their own copy, renaming one would silently break a
  dashboard instead of failing a test.
- **Registries are injectable.** Tests build a `FlowMeshMetrics` on a fresh
  `CollectorRegistry` and read exact values out of it. `prometheus_client`
  raises `Duplicated timeseries` if you register the same name twice in one
  registry, so a module-level singleton would make a second test impossible.

The invariant that matters: **metrics can never fail the operation they
measure.** Every update goes through a helper that swallows instrumentation
errors and counts them, because a `KeyError` from a renamed label in a hot
loop should cost you a number, not an order.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from backend.config.settings import get_settings

# Label names that appear across metrics. Declared here so a typo in a label
# set is an AttributeError at import time rather than a silently separate
# time series at runtime.
BAND = "band"
OUTCOME = "outcome"
QUEUE = "queue"
TOPIC = "topic"
GROUP = "group"
WAREHOUSE = "warehouse"
TRANSPORT = "transport"
REASON = "reason"
TABLE = "table"
STATE = "state"

# The `state` label's permitted values, as constants. Named rather than indexed
# into a tuple so a typo is a NameError at import time, and so a Grafana panel's
# `state="ready"` is a value this file also names.
STATE_READY = "ready"
STATE_UNACKED = "unacked"
STATE_DEAD_LETTER = "dead_letter"

# Risk bands, mirrored from `backend.events.schema.RiskBand` as plain strings.
# `prometheus_client` label values must be strings, and duplicating them here
# rather than importing the enum keeps `metrics.py` free of application imports --
# it is the module the observability stack depends on, so it must not depend on
# the application.
BANDS = ("low", "ambiguous", "high")


class InstrumentationFailure(Exception):
    """Raised only by the self-test that proves metrics cannot break callers."""


class FlowMeshMetrics:
    """Every metric FlowMesh exposes, on one registry."""

    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        *,
        enabled: bool = True,
        scoring_buckets: tuple[float, ...] | None = None,
    ) -> None:
        settings = get_settings()
        self.registry = registry if registry is not None else CollectorRegistry()
        self.enabled = enabled
        buckets = scoring_buckets or settings.scoring_latency_buckets

        # --- Ingress ---
        self.orders_ingested = Counter(
            "flowmesh_orders_ingested_total",
            "Orders accepted by the public API and published to the event log.",
            [OUTCOME],
            registry=self.registry,
        )
        self.orders_idempotent_hits = Counter(
            "flowmesh_orders_idempotent_hits_total",
            "Requests that matched an existing Idempotency-Key and were not republished.",
            registry=self.registry,
        )
        self.rate_limited = Counter(
            "flowmesh_rate_limited_total",
            "Requests rejected by the Redis rate limiter.",
            registry=self.registry,
        )
        self.api_request_latency = Histogram(
            "flowmesh_api_request_seconds",
            "Latency of public API requests.",
            ["endpoint"],
            registry=self.registry,
        )

        # --- Event log ---
        self.events_published = Counter(
            "flowmesh_events_published_total",
            "Events appended to the event log.",
            [TOPIC],
            registry=self.registry,
        )
        self.events_consumed = Counter(
            "flowmesh_events_consumed_total",
            "Events handed to a handler, including redeliveries.",
            [TOPIC, GROUP, OUTCOME],
            registry=self.registry,
        )
        self.events_redelivered = Counter(
            "flowmesh_events_redelivered_total",
            "Events delivered again after a failure or an uncommitted offset.",
            [TOPIC, GROUP],
            registry=self.registry,
        )
        self.events_deduplicated = Counter(
            "flowmesh_events_deduplicated_total",
            "Events whose effect was already applied and were skipped.",
            [TOPIC],
            registry=self.registry,
        )
        self.consumer_lag = Gauge(
            "flowmesh_consumer_lag",
            "Records behind a consumer group, by topic.",
            [TOPIC, GROUP],
            registry=self.registry,
        )
        self.commit_latency = Histogram(
            "flowmesh_offset_commit_seconds",
            "Time to commit an offset after a handler returns.",
            [TOPIC, GROUP],
            registry=self.registry,
        )

        # --- Fraud scoring ---
        self.scoring_latency = Histogram(
            "flowmesh_scoring_latency_seconds",
            "End-to-end fraud scoring latency, including the gRPC hop when there is one.",
            [TRANSPORT],
            buckets=buckets,
            registry=self.registry,
        )
        self.orders_scored = Counter(
            "flowmesh_orders_scored_total",
            "Scoring decisions, by risk band.",
            [BAND, TRANSPORT],
            registry=self.registry,
        )
        self.score_distribution = Histogram(
            "flowmesh_score_distribution",
            "Distribution of fraud scores. Fraud rate is derived from this, not "
            "stored as its own gauge, so the two can never disagree.",
            buckets=tuple(index / 20 for index in range(1, 21)),
            registry=self.registry,
        )
        self.scoring_errors = Counter(
            "flowmesh_scoring_errors_total",
            "Scoring attempts that failed or timed out.",
            [REASON],
            registry=self.registry,
        )
        self.scoring_degraded = Counter(
            "flowmesh_scoring_degraded_total",
            "Orders scored by a fallback path rather than the real model.",
            [REASON],
            registry=self.registry,
        )

        # --- LLM reasoner ---
        self.llm_calls = Counter(
            "flowmesh_llm_calls_total",
            "LLM rationale calls. One per ambiguous order at most.",
            [OUTCOME],
            registry=self.registry,
        )
        self.llm_latency = Histogram(
            "flowmesh_llm_latency_seconds",
            "LLM rationale latency.",
            buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0),
            registry=self.registry,
        )
        self.llm_prompt_tokens = Counter(
            "flowmesh_llm_prompt_tokens_total",
            "Prompt tokens sent, for cost accounting.",
            registry=self.registry,
        )
        self.llm_completion_tokens = Counter(
            "flowmesh_llm_completion_tokens_total",
            "Completion tokens received, for cost accounting.",
            registry=self.registry,
        )

        # --- Task queue ---
        self.queue_depth = Gauge(
            "flowmesh_queue_depth",
            "Messages waiting, by queue and state.",
            [QUEUE, STATE],
            registry=self.registry,
        )
        self.queue_published = Counter(
            "flowmesh_queue_published_total",
            "Messages published to a task queue.",
            [QUEUE],
            registry=self.registry,
        )
        self.queue_processed = Counter(
            "flowmesh_queue_processed_total",
            "Task-queue messages handled.",
            [QUEUE, OUTCOME],
            registry=self.registry,
        )
        self.queue_dead_lettered = Counter(
            "flowmesh_queue_dead_lettered_total",
            "Messages moved to a dead-letter queue after exhausting retries.",
            [QUEUE],
            registry=self.registry,
        )
        self.notifications_sent = Counter(
            "flowmesh_notifications_sent_total",
            "Customer and reviewer notifications emitted.",
            [REASON],
            registry=self.registry,
        )

        # --- Inventory ---
        self.stock_reserved = Counter(
            "flowmesh_stock_reserved_total",
            "Units reserved by warehouse.",
            [WAREHOUSE],
            registry=self.registry,
        )
        self.stock_released = Counter(
            "flowmesh_stock_released_total",
            "Units returned to available by warehouse.",
            [WAREHOUSE],
            registry=self.registry,
        )
        self.oversell_rejections = Counter(
            "flowmesh_oversell_rejections_total",
            "Reservations refused because the warehouse had insufficient stock.",
            [WAREHOUSE],
            registry=self.registry,
        )
        self.reservation_expirations = Counter(
            "flowmesh_reservation_expirations_total",
            "Reservations reclaimed after exceeding their TTL.",
            registry=self.registry,
        )
        # A heartbeat, and the only trustworthy way to alert on the sweeper being
        # dead. The alternatives all fail the same way: "expirations stopped" is
        # also what a quiet night looks like, and "stock_reserved_total is not
        # rising" is also what a healthy system does for hours. This is a Unix
        # timestamp the sweeper writes every pass, so
        # `time() - flowmesh_reservation_sweep_last_success_unixtime > 300` fires
        # on the failure and only on the failure.
        #
        # A gauge rather than a counter precisely so it can go *backwards* in the
        # data (a restart) without a counter's monotonicity being a lie about
        # uptime.
        self.reservation_sweep_last_success = Gauge(
            "flowmesh_reservation_sweep_last_success_unixtime",
            "Unix time of the most recent reservation-expiry sweep.",
            registry=self.registry,
        )
        self.inventory_apply_latency = Histogram(
            "flowmesh_inventory_apply_seconds",
            "Latency of applying one inventory event to the ledger.",
            registry=self.registry,
        )

        # --- Database ---
        self.db_write_latency = Histogram(
            "flowmesh_db_write_seconds",
            "Latency of the transaction that persists an order or a score.",
            [TABLE],
            registry=self.registry,
        )

        # --- Instrumenting the instrumenting ---
        self.instrumentation_failures = Counter(
            "flowmesh_instrumentation_failures_total",
            "Metric updates that raised and were swallowed. Should be 0; if it "
            "is not, a label set has drifted.",
            registry=self.registry,
        )

    # -- the two helpers everything else uses -----------------------------

    def _guard(self, operation: str, call: Any) -> None:
        """Run a metric update, counting -- not merely swallowing -- bugs.

        The failure counter is not bookkeeping, it is the thing that makes this
        guard safe. `prometheus_client` has two ways to record a labelled sample
        and only one of them is obvious:

            `metric.labels(a, b).observe(x)`   -- correct
            `metric.observe(x, a, b)`           -- TypeError
            `metric.observe(x, a=a)`            -- TypeError

        The wrong forms raise, and a guard that only swallowed the error would
        leave a whole metric family permanently empty: no exception anywhere, no
        panel on the dashboard, and every budget assertion in this project
        measuring nothing while appearing to pass. That is not hypothetical -- it
        is exactly what this code did, and
        `tests/unit/fraud/test_engine.py::test_scoring_latency_is_recorded` is
        what caught it.

        So: `TypeError` and `ValueError` are re-raised, because both mean the call
        site is wrong rather than that the sample is inconvenient. Genuine
        runtime noise is still swallowed, and counted.
        """
        if not self.enabled:
            return
        try:
            call()
        except (TypeError, ValueError):
            # Wrong arity, a bad label name, or a label value the metric does not
            # declare. A programming error: re-raise it.
            self._count_instrumentation_failure()
            raise
        except Exception:  # noqa: BLE001 - everything else is instrumentation noise
            self._count_instrumentation_failure()

    def _count_instrumentation_failure(self) -> None:
        try:
            self.instrumentation_failures.inc()
        except Exception:  # noqa: BLE001 - the counter itself is best-effort
            pass

    @asynccontextmanager
    async def timer(self, metric: Histogram, *labels: str) -> AsyncIterator[Any]:
        """Observe a duration on `metric`, re-raising on the way out.

        `metric` is the unlabelled `Histogram`; labels are applied here through
        `.labels(...)`. Passing an already-labelled child also works.

        It does not suppress: a timer that swallowed the exception would turn a
        failed scoring call into a successful-looking slow one.

        `@asynccontextmanager`, not `@contextmanager` -- this yields from an
        async body, and a plain `@contextmanager` produces an object with
        `__enter__` but no `__aenter__`, which fails at the call site rather than
        at import time.
        """
        from backend.core.clock import monotonic

        start = monotonic()
        try:
            yield
        finally:
            elapsed = monotonic() - start
            self._guard("timer", lambda: metric.labels(*labels).observe(elapsed))

    def record_scoring(
        self,
        *,
        score: float,
        band: str,
        latency_seconds: float,
        transport: str,
    ) -> None:
        """One scoring decision: latency, band, and the score itself.

        Called from one place (`backend/fraud/engine.py`) so a decision can
        never be counted without its latency or vice versa. The band is passed
        in rather than recomputed from the score because the caller owns the
        thresholds -- a second `band_for()` call here would be a second place to
        get the boundary wrong, and the boundary is the whole policy.
        """
        self._guard(
            "record_scoring",
            lambda: self.scoring_latency.labels(transport).observe(latency_seconds),
        )
        self._guard(
            "record_scoring",
            lambda: self.orders_scored.labels(band=band, transport=transport).inc(),
        )
        self._guard("record_scoring", lambda: self.score_distribution.observe(score))

    def set_queue_depth(self, queue: str, ready: int, unacked: int, dead_letter: int = 0) -> None:
        """Publish queue depth for one queue, in all three states.

        The three states are separate series rather than one number because they
        mean different things operationally: a growing `ready` is a backlog, a
        growing `unacked` is slow handlers, and a non-zero `dead_letter` is poison
        messages. A single summed gauge hides all three.
        """
        for state, value in (
            (STATE_READY, ready),
            (STATE_UNACKED, unacked),
            (STATE_DEAD_LETTER, dead_letter),
        ):
            self._guard(
                "set_queue_depth",
                lambda s=state, v=value: self.queue_depth.labels(queue=queue, state=s).set(v),
            )

    def render(self) -> bytes:
        """The exposition format, exactly as `/metrics` serves it."""
        return generate_latest(self.registry)

    def value_of(self, name: str, *, default: float | None = None, **labels: str) -> float:
        """Read one sample back out of the registry.

        For tests and reports that assert on a *metric* rather than on a mock's
        call list. A dashboard is not evidence; a number you can fail a build on
        is.

        Labels match **exactly**. A histogram's `_count`/`_sum`/`_bucket` samples
        carry their series' labels, so a subset match would happily return a
        bucket boundary when asked for a series.

        `default` is for asserting that something did *not* happen. A labelled
        counter has no sample at all until something increments it, so
        "no LLM call was made" cannot be expressed as "read the sample and expect
        0" -- the sample is absent, and absent is not zero. Passing
        `default=0.0` states the intent; omitting it raises, which is the right
        behaviour when the caller expected the sample to exist.
        """
        samples = [
            sample
            for metric in self.registry.collect()
            for sample in metric.samples
            if sample.name == name and dict(sample.labels) == dict(labels)
        ]
        if not samples:
            if default is not None:
                return default
            raise InstrumentationFailure(
                f"no sample named {name!r} with exactly {labels!r}. Either nothing "
                "has been recorded yet (pass default=0.0 to assert absence), or "
                "the label set does not match -- matching is exact because a "
                "histogram's _count/_sum/_bucket samples share their series' labels"
            )
        return float(samples[0].value)


@lru_cache
def get_metrics() -> FlowMeshMetrics:
    """Return the process-wide metrics singleton.

    `lru_cache` because a `Counter` may only be registered once per registry,
    so building a second one against the default registry raises at
    construction. Tests that need their own call `FlowMeshMetrics(CollectorRegistry())`.
    """
    settings = get_settings()
    return FlowMeshMetrics(enabled=settings.metrics_enabled)


def reset_metrics() -> None:
    """Drop the singleton so the next `get_metrics()` builds a fresh one."""
    get_metrics.cache_clear()
