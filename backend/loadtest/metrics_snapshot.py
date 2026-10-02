"""Snapshot the metrics registry into plain numbers.

The load-test report is written to disk and read by people who are not running the
code, so it has to contain values rather than references to a registry. This
module is the bridge, and it exists separately from `FlowMeshMetrics` so the
report format can change without touching the metric definitions.

It reads the registry rather than keeping parallel counters, deliberately. A
load test that maintained its own tallies alongside the metrics would produce two
numbers that agree until they do not, and the disagreement would be found by
whoever read the wrong one.
"""

from __future__ import annotations

from typing import Any

from prometheus_client import CollectorRegistry


def _series(registry: CollectorRegistry, name: str) -> list[dict[str, Any]]:
    """Every sample whose *sample* name is `name`, as `{labels, value}` dicts.

    Matching on the **sample** name, not the family's. `prometheus_client` yields
    one `Metric` per family, and a histogram family called
    `flowmesh_scoring_latency_seconds` produces samples called
    `..._bucket`, `..._count` and `..._sum`. Comparing `metric.name` against a
    suffixed sample name therefore matches nothing at all, and the first version of
    this function did exactly that -- which made the load report claim a 0.00ms
    p95 on a run that had scored 3,000 orders in 0.07ms each. The `_empty` flag
    below exists so that class of mistake is loud rather than flattering.
    """
    found: list[dict[str, Any]] = []
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name != name:
                continue
            found.append({"labels": dict(sample.labels), "value": float(sample.value)})
    return found


def _histogram(registry: CollectorRegistry, name: str, label_key: str) -> dict[str, float]:
    """Percentiles reconstructed from a histogram's cumulative `_bucket` samples.

    **These are bucket upper bounds, not measurements.** A Prometheus histogram
    records only "how many observations fell at or below each edge", so the best
    it can say about a p95 is the edge of the bucket the 95th percentile landed
    in. A reported p95 of 5ms means "at or below 5ms" -- not "5.0ms", and
    definitely not "5.00ms".

    That is reported as-is rather than smoothed, because the number a dashboard
    shows and the number a load test budgets against have to be the same number,
    and a "reconstruction" that interpolated between edges would differ from both.
    The `p95_is_upper_bound` flag in the result is there so the report can say so
    in words.

    Reading the buckets rather than the scoring engine's own sample list is
    deliberate: this is what Prometheus actually scrapes. If the configured buckets
    could not resolve the 200ms budget, the run would not find out here.
    """
    import bisect

    buckets: list[tuple[float, float]] = []
    for sample in _series(registry, f"{name}_bucket"):
        bucket_label = sample["labels"].get(label_key)
        if bucket_label is None:
            continue
        upper = float("inf") if bucket_label == "+Inf" else float(bucket_label)
        buckets.append((upper, sample["value"]))
    if not buckets:
        return {
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
            "observations": 0.0,
            "empty": True,
        }
    buckets.sort()
    upper_bounds = [bound for bound, _ in buckets]
    # The total is the highest *finite* bucket's cumulative count, or the `+Inf`
    # one if present. Taking `buckets[-1][1]` after sorting is subtly wrong:
    # sorting puts `+Inf` last, so it happens to work -- but only because the
    # sort is by upper bound, and that is an accident rather than a reason.
    finite = [(bound, count) for bound, count in buckets if bound != float("inf")]
    observations = max((count for _, count in finite), default=0.0) if finite else buckets[-1][1]

    def _quantile(fraction: float) -> float:
        if observations <= 0:
            return 0.0
        target = fraction * observations
        index = bisect.bisect_left([cumulative for _, cumulative in buckets], target)
        index = min(index, len(upper_bounds) - 1)
        return upper_bounds[index]

    # `max` is the last finite bucket, not `+Inf`. `+Inf` is an upper bound, not
    # an observation, and reporting it as a maximum would put a meaningless
    # "infinity" in the report.
    largest_finite = max((bound for bound, _ in finite), default=0.0)
    # A true maximum is unknowable from a histogram: an observation above the last
    # finite edge is only known to be below `+Inf`. The largest finite edge is
    # reported instead, and `max_is_lower_bound` says which it is, because a reader
    # who takes "max = 2000ms" as a measurement will draw the wrong conclusion.
    return {
        "p50": _quantile(0.50) * 1000.0,
        "p95": _quantile(0.95) * 1000.0,
        "p99": _quantile(0.99) * 1000.0,
        "max": largest_finite * 1000.0,
        "observations": observations,
        "p95_is_upper_bound": True,
        "max_is_lower_bound": True,
        "empty": False,
    }


def _counter_sum(registry: CollectorRegistry, name: str) -> float:
    return sum(sample["value"] for sample in _series(registry, name))


def snapshot_metrics(metrics: Any) -> dict[str, Any]:
    """The numbers the load-test report quotes.

    `metrics` is a `FlowMeshMetrics`; it is typed loosely here so this module has
    no import edge into the metrics definitions, and so a test can pass a bare
    registry.
    """
    registry: CollectorRegistry = metrics.registry
    by_band = {
        sample["labels"].get("band", "?"): sample["value"]
        for sample in _series(registry, "flowmesh_orders_scored_total")
    }
    return {
        "scoring_latency": _histogram(registry, "flowmesh_scoring_latency_seconds", "le"),
        "scored_total": _counter_sum(registry, "flowmesh_orders_scored_total"),
        "by_band": by_band,
        "degraded": _counter_sum(registry, "flowmesh_scoring_degraded_total"),
        "llm_calls": _counter_sum(registry, "flowmesh_llm_calls_total"),
        "oversell_rejections": _counter_sum(registry, "flowmesh_oversell_rejections_total"),
        "duplicates": _counter_sum(registry, "flowmesh_events_deduplicated_total"),
        "redeliveries": _counter_sum(registry, "flowmesh_events_redelivered_total"),
        "instrumentation_failures": _counter_sum(
            registry, "flowmesh_instrumentation_failures_total"
        ),
    }
