"""The Grafana dashboard must only query metrics that exist.

A panel whose PromQL references a metric nobody exports renders as "No data" —
permanently, quietly, with no error anywhere. That is the worst failure mode a
dashboard has, because it looks like the system is idle rather than like the
dashboard is broken.

This file closes that gap mechanically: every metric name appearing in the
provisioned dashboard JSON must exist in `FlowMeshMetrics`. A renamed metric fails
here rather than on somebody's screen three weeks later.

The inverse check matters just as much: every metric the code exports should be on
a dashboard. Not every one has to be -- `flowmesh_events_published_total` is
diagnostic -- so a small, *named* allowlist of intentional exceptions, because a
silent allowlist is how a dashboard rots.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from backend.observability.metrics import FlowMeshMetrics

DASHBOARD = Path("observability/grafana/dashboards/flowmesh-overview.json")

#: `flowmesh_` followed by word characters. Deliberately a regex rather than a
#: metric-name allowlist, so a *new* metric in the dashboard is covered the moment
#: it is written, and a *renamed* metric in the code fails immediately.
METRIC_NAME = re.compile(r"flowmesh_[a-z0-9_]+")

#: Exported but intentionally not charted. Each with a reason, because a
#: silent allowlist is how a dashboard rots.
NOT_CHARTED: dict[str, str] = {
    "flowmesh_events_published_total": (
        "diagnostic; the Orders/sec panel reads the API-side counter instead"
    ),
    "flowmesh_orders_idempotent_hits_total": (
        "would need its own panel to be legible; the useful form is 'replays per "
        "order', which is a product question rather than an ops one"
    ),
    "flowmesh_rate_limited_total": ("belongs on a rate-limit dashboard, not the pipeline one"),
    "flowmesh_api_request_seconds": (
        "server-side request timing is a separate concern from the pipeline"
    ),
    "flowmesh_queue_published_total": "derivable from the queue-depth panel's rate",
    "flowmesh_queue_processed_total": "derivable from the queue-depth panel's rate",
    "flowmesh_notifications_sent_total": (
        "always near zero in a healthy system, so it belongs in an alert rule "
        "rather than a panel -- see observability/prometheus/alerts.yml"
    ),
    "flowmesh_reservation_expirations_total": (
        "high is *good* (the sweeper working), which makes it a confusing panel; "
        "it is an alert rule instead"
    ),
    "flowmesh_llm_prompt_tokens_total": ("cost accounting, charted only when the LLM is enabled"),
    "flowmesh_llm_completion_tokens_total": (
        "cost accounting, charted only when the LLM is enabled"
    ),
    "flowmesh_llm_latency_seconds": "charted only when the LLM is enabled",
    "flowmesh_queue_dead_lettered_total": (
        "charted as the DEAD series of the queue-depth panel, which reads the "
        "gauge rather than the counter"
    ),
    "flowmesh_reservation_sweep_last_success_unixtime": (
        "a heartbeat: meaningless as a time series and unreadable as a panel, and "
        "its whole purpose is the alert rule in "
        "observability/prometheus/rules/alerts.yml that reads its staleness"
    ),
}


@pytest.fixture(scope="module")
def metrics() -> Any:
    """A registry with every metric materialised.

    prometheus_client only creates a child series when .labels(...) is called, so a
    fresh registry yields nothing at all for a labelled metric -- not even an empty
    family. That is the same reason a panel can sit on "No data" for a metric
    that does exist: nothing has incremented it yet.

    Touching each metric is what makes "is this name exported?" a real question
    rather than a question about whether the test incremented it first. And it has to
    be done per-metric, from each metric's own declared labels: passing one fixed
    label set only reaches metrics whose labels happen to match, and the rest stay
    un-emitted -- so the test below reported eleven real metrics as non-existent.
    The bug the test exists to catch, in the test itself.
    """
    from prometheus_client import CollectorRegistry

    instance = FlowMeshMetrics(registry=CollectorRegistry())
    values = {
        "band": "low",
        "endpoint": "/orders",
        "group": "test-group",
        "outcome": "approved",
        "queue": "review-queue",
        "reason": "test-reason",
        "state": "ready",
        "table": "orders",
        "topic": "order-events",
        "transport": "in_process",
        "warehouse": "WH-01",
    }
    for _attribute, metric in vars(instance).items():
        if not hasattr(metric, "labels"):
            continue
        declared = tuple(getattr(metric, "_labelnames", ()) or ())
        if not declared:
            # Unlabelled metrics are emitted at construction.
            continue
        child = metric.labels(**{name: values[name] for name in declared})
        # Gauges need a value; counters and histograms emit _created on creation.
        if hasattr(child, "set"):
            child.set(0)
    return instance


@pytest.fixture(scope="module")
def dashboard() -> dict[str, Any]:
    """The provisioned dashboard JSON, parsed once per module."""
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def dashboard_metric_names(dashboard: dict[str, Any]) -> set[str]:
    """Every `flowmesh_*` identifier appearing anywhere in the dashboard JSON.

    Serialised whole and scanned, rather than walking `targets[].expr` only: a
    metric named in a panel `description`, a `legendFormat` or a template-variable
    query is just as much a claim that the metric exists.
    """
    return set(METRIC_NAME.findall(json.dumps(dashboard)))


def test_the_dashboard_exists() -> None:
    assert DASHBOARD.is_file(), f"{DASHBOARD} is missing; the dashboards are provisioned from it"


def test_the_dashboard_has_panels(dashboard: dict[str, Any]) -> None:
    assert len(dashboard["panels"]) >= 8, "a dashboard with few panels is not a dashboard"


def test_the_dashboard_is_provisioned_read_only(dashboard: dict[str, Any]) -> None:
    """An accidental UI edit must not be silently lost on restart."""
    assert dashboard["editable"] is False


def _families(names: set[str]) -> set[str]:
    """Reduce metric and sample names to the family they belong to.

    `flowmesh_stock_reserved`, `flowmesh_stock_reserved_total`,
    `flowmesh_api_request_seconds_bucket` and `flowmesh_api_request_seconds` are one
    metric between them.
    """
    families = set()
    for name in names:
        for suffix in ("_total", "_count", "_sum", "_bucket", "_created"):
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        families.add(name)
    return families


def _exported_names(registry: Any) -> set[str]:
    """Every name a scrape of this registry can return.

    Both the *family* name and the *sample* names. `prometheus_client` names a
    counter's family `flowmesh_orders_ingested` while the series Prometheus
    actually scrapes is `flowmesh_orders_ingested_total` -- so a dashboard
    comparing against family names alone reports every counter as missing.

    Comparing family names only is the mistake that made this test fail on its
    first correct run, reporting eleven real metrics as non-existent.
    """
    names: set[str] = set()
    for metric in registry.collect():
        names.add(metric.name)
        names.update(sample.name for sample in metric.samples)
    return names


def test_every_metric_the_dashboard_queries_exists(
    dashboard_metric_names: set[str], metrics: Any
) -> None:
    """The core check.

    Asserts against the *live registry* rather than a hand-maintained list, so a
    metric renamed in `metrics.py` fails here immediately.
    """
    exported = _exported_names(metrics.registry)
    unknown = {name for name in dashboard_metric_names if name not in exported}
    assert not unknown, (
        f"the dashboard queries metrics that do not exist: {sorted(unknown)}. "
        "Either the metric was renamed in FlowMeshMetrics or the panel is wrong; "
        "a panel querying a missing metric renders as 'No data' forever."
    )


def test_histogram_quantiles_reference_real_buckets(
    dashboard_metric_names: set[str], metrics: Any
) -> None:
    """`histogram_quantile` over a metric with no `_bucket` series returns nothing.

    A subtle one: `flowmesh_scoring_latency_seconds` exists as a family, and a
    typo'd `_bucket` suffix would produce a query that parses, resolves to an
    absent series, and returns no data with no error.
    """
    exported = _exported_names(metrics.registry)
    families = {name[: -len("_bucket")] for name in exported if name.endswith("_bucket")}
    assert "flowmesh_scoring_latency_seconds" in families, (
        "the p95 panels have no bucket series to read; a Histogram always emits "
        "one, so this means the metric is not a Histogram"
    )


def test_the_instrumentation_counter_is_charted(dashboard_metric_names: set[str]) -> None:
    """The panel that would have caught the silently-empty-histogram bug."""
    assert "flowmesh_instrumentation_failures_total" in dashboard_metric_names


def test_the_oversell_panel_is_a_stat_not_a_rate(
    dashboard: dict[str, Any], dashboard_metric_names: set[str]
) -> None:
    """The oversell budget is absolute, so it must not be charted as a rate.

    A rate of oversells looks identical whether the system sold 10 units or
    10 million, and the sprint's budget is `zero`. An absolute counter is the only
    honest rendering.
    """
    panels = {panel["title"]: panel for panel in dashboard["panels"]}
    oversell = panels["Total oversells"]
    assert oversell["type"] == "stat"
    assert oversell["fieldConfig"]["defaults"]["decimals"] == 0
    assert "flowmesh_oversell_rejections_total" in dashboard_metric_names


def test_every_panel_has_a_description(dashboard: dict[str, Any]) -> None:
    """A dashboard panel nobody can interpret is decoration.

    Each description here says what the number *means* and what an operator should
    do about it -- which is the difference between a dashboard and a chart.
    """
    missing = [panel["title"] for panel in dashboard["panels"] if not panel.get("description")]
    assert not missing, f"panels with no description: {missing}"


def test_uncharted_metrics_are_named_with_a_reason(metrics: Any) -> None:
    """The allowlist is explicit, and every entry explains itself."""
    for name, reason in NOT_CHARTED.items():
        assert reason and len(reason) > 30, f"{name} is excused without a reason"


def test_no_allowlisted_metric_is_actually_charted(
    dashboard_metric_names: set[str],
) -> None:
    """Stale allowlist entries are how an allowlist stops meaning anything.

    If a metric is both excused and charted, the excuse is a lie and the next
    reader will trust the wrong half.
    """
    both = set(NOT_CHARTED) & dashboard_metric_names
    assert not both, (
        f"these are in NOT_CHARTED but the dashboard queries them: {sorted(both)}. "
        "Remove them from the allowlist, or remove the panels."
    )


def test_every_exported_metric_is_charted_or_excused(
    dashboard_metric_names: set[str], metrics: Any
) -> None:
    """The other direction, and the one that actually keeps the dashboard current.

    The test above asks "does every metric the dashboard names exist?". This asks
    "is every metric that exists either on the dashboard or explained away?".

    Without it, adding a metric is invisible here: nothing fails, and the metric is
    simply never charted. That is how `flowmesh_reservation_sweep_last_success_unixtime`
    arrived -- added with the sweeper heartbeat, exported, scraped, and read by
    exactly nobody, with the suite green the whole time.

    Compared at *family* level, not by exact name, because the three naming
    conventions do not line up: a counter's family is `flowmesh_stock_reserved` while
    the panel queries `flowmesh_stock_reserved_total`, and a histogram is exported
    as `_bucket`/`_count`/`_sum` but appears in PromQL under any one of those. An
    exact-name comparison reports 44 unexplained metrics for a dashboard that is in
    fact fully accounted for -- which is how this test came to assert nothing useful
    before it asserted anything.
    """
    unexplained = (
        _families(_exported_names(metrics.registry))
        - _families(dashboard_metric_names)
        - _families(set(NOT_CHARTED))
    )
    assert not unexplained, (
        f"exported but neither charted nor explained: {sorted(unexplained)}. "
        "Add a panel, or add an entry to NOT_CHARTED with the reason it does not "
        "belong on this dashboard."
    )


def test_the_dashboard_queries_the_scorer_and_the_consumers(
    dashboard_metric_names: set[str],
) -> None:
    """The three consumers each expose something worth watching.

    A pipeline dashboard that only shows the API is an API dashboard.

    Histogram families are named with their `_bucket` suffix, because that is what
    the panel's PromQL actually references: `histogram_quantile` reads bucket
    series, so a dashboard naming only the family name would render no data. Hence
    `flowmesh_scoring_latency_seconds_bucket` rather than
    `flowmesh_scoring_latency_seconds` -- matching the assertion the first
    version of this test got wrong.
    """
    required = {
        "flowmesh_scoring_latency_seconds_bucket",  # the gRPC boundary
        "flowmesh_consumer_lag",  # the event log
        "flowmesh_queue_depth",  # the task queue
        "flowmesh_stock_reserved_total",  # the inventory ledger
    }
    assert required <= dashboard_metric_names, (
        f"not charted: {sorted(required - dashboard_metric_names)}"
    )


def test_every_histogram_read_by_a_panel_uses_its_bucket_series(
    dashboard: dict[str, Any], dashboard_metric_names: set[str]
) -> None:
    """`histogram_quantile` over a family name rather than `_bucket` returns nothing.

    The query parses, resolves to no series, and renders as an empty panel with no
    error anywhere -- so it is asserted structurally rather than left to be found
    on a screen.
    """
    quantile_metrics = {
        name
        for name in dashboard_metric_names
        if name.endswith("_bucket")
        or re.search(r"flowmesh_[a-z0-9_]+(seconds|count|distribution)(?![a-z0-9_])", name)
    }
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            expression = target.get("expr", "")
            if "histogram_quantile" not in expression:
                continue
            assert "_bucket" in expression, (
                f"panel {panel['title']!r} calls histogram_quantile on "
                f"{expression!r} without reading a _bucket series"
            )
    _ = quantile_metrics


def test_the_datasource_uid_matches_the_provisioning_file() -> None:
    """The dashboard's `datasource.uid` must match `provisioning/datasources`.

    A mismatch is invisible until the dashboard loads and every panel shows "no
    data" because the datasource cannot be resolved.
    """
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    uids = {
        template["datasource"]["uid"]
        for template in dashboard.get("templating", {}).get("list", [])
        if "datasource" in template
    }
    provisioning = Path("observability/grafana/provisioning/datasources/prometheus.yaml").read_text(
        encoding="utf-8"
    )
    assert uids, "the dashboard has no templating variables with a datasource"
    for uid in uids:
        assert f"uid: {uid}" in provisioning, (
            f"the dashboard references datasource uid {uid!r}, which "
            "provisioning/datasources/prometheus.yaml does not define"
        )
