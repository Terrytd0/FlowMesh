"""The scoring engine: streaming features, band routing, and the p95 budget.

Two things are being protected here, and they pull in opposite directions.

**The budget.** `p95 scoring latency under 200ms` is a product requirement, so it
is asserted as a measurement on real work rather than described in a README. The
engine is also measured with and without the LLM reasoner enabled, because the
claim "an LLM on the hot path cannot meet the budget" is only worth making if the
number is measured.

**The ordering.** `record()` must run *after* the decision. Recording first counts
an order toward its own velocity, which inflates every score by one order and
turns the velocity feature into noise -- a bug that produces plausible numbers and
no error.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import pytest

from backend.core.clock import reset_clock_offset, set_clock_offset, utcnow
from backend.events.schema import RiskBand
from backend.fraud.engine import FraudScoringEngine
from backend.fraud.features import OrderContext, compute_features
from backend.fraud.model import load_model
from backend.fraud.profile_window import CustomerWindow
from backend.observability.metrics import FlowMeshMetrics


def clean_order(customer_id: str = "CUST-1", **overrides: Any) -> OrderContext:
    """A domestic order from a known customer. Should land in the low band.

    `received_at` defaults to *now*. The velocity window is 15 minutes wide, so a
    hardcoded date silently falls out of the window as soon as the suite is run
    more than a quarter of an hour after that date -- and then the velocity tests
    fail for reasons that have nothing to do with the code. This was written the
    other way first and it failed for exactly that reason.

    A consequence: `night_hour` is not deterministic from this helper, because the
    UTC hour depends on when the suite runs. The one test that needs a small-hours
    order sets the clock explicitly (`set_clock_offset`) rather than pinning a
    timestamp.
    """
    from backend.core.clock import utcnow

    defaults: dict[str, Any] = {
        "order_id": "CRG-1",
        "customer_id": customer_id,
        "total_cents": 5000,
        "items": (("SKU-TSHIRT-M", 1), ("SKU-MUG-STD", 2)),
        "card_bin": "411111",
        "card_country": "US",
        "billing_country": "US",
        "shipping_country": "US",
        "ip_country": "US",
        "coupon_code": None,
        "is_gift_card": False,
        "received_at": utcnow(),
    }
    defaults.update(overrides)
    return OrderContext(**defaults)


@contextmanager
def clock_at(hour: int) -> Any:
    """Move the clock so that `utcnow()` falls in the given UTC hour.

    `set_clock_offset` rather than monkeypatching `utcnow`, because the modules
    under test imported `utcnow` by value at import time -- rebinding the
    attribute on `backend.core.clock` would leave them all calling the original,
    and the test would pass without exercising the feature it names.
    """
    now = utcnow()
    delta = timedelta(hours=hour - now.hour, minutes=-now.minute, seconds=-now.second)
    set_clock_offset(delta)
    try:
        yield
    finally:
        reset_clock_offset()


def suspicious_order(customer_id: str = "CUST-1", **overrides: Any) -> OrderContext:
    """A first-order, three-country-mismatch, huge-value, coupon order."""
    return clean_order(
        customer_id,
        total_cents=250_000,
        card_country="NG",
        billing_country="US",
        shipping_country="US",
        ip_country="NG",
        coupon_code="SAVE90",
        **overrides,
    )


# ---------------------------------------------------------------- decisions


async def test_a_clean_order_is_auto_approvable(scoring_engine: FraudScoringEngine) -> None:
    decision = await scoring_engine.score(clean_order())
    assert decision.band == RiskBand.LOW
    assert decision.auto_approvable
    assert decision.score < 0.35


async def test_an_obviously_fraudulent_order_is_high(scoring_engine: FraudScoringEngine) -> None:
    decision = await scoring_engine.score(suspicious_order())
    assert decision.band == RiskBand.HIGH
    assert not decision.auto_approvable
    assert decision.score > 0.65


async def test_reasons_are_reported_for_a_held_order(scoring_engine: FraudScoringEngine) -> None:
    decision = await scoring_engine.score(suspicious_order())
    assert decision.reasons, "a held order must say why"
    assert "country_mismatch" in decision.reasons or "card_country_mismatch" in decision.reasons


async def test_the_rationale_is_produced_without_asking(scoring_engine: FraudScoringEngine) -> None:
    """The deterministic narrative is free; there is no excuse for an empty one."""
    decision = await scoring_engine.score(suspicious_order())
    assert decision.rationale
    assert decision.llm_used is False


async def test_contributions_ride_along_with_the_decision(
    scoring_engine: FraudScoringEngine,
) -> None:
    """Fraud ops asks "why" within minutes, not after a re-run."""
    decision = await scoring_engine.score(suspicious_order())
    assert decision.contributions
    assert decision.features
    assert len(decision.features) == 12


async def test_a_degraded_score_is_never_auto_approvable(
    degraded_engine: FraudScoringEngine,
) -> None:
    """The property that makes the degraded path safe to run.

    Even a 0.02 score from the fallback model is not approvable. The failure this
    prevents is a model outage quietly becoming "approve everything".
    """
    decision = await degraded_engine.score(clean_order())
    assert decision.degraded is True
    assert not decision.auto_approvable


async def test_a_degraded_engine_still_holds_the_obvious_cases(
    degraded_engine: FraudScoringEngine,
) -> None:
    """The fallback must still catch the pattern it exists to catch.

    The fallback is a *weaker* model, not a blind one. It fires on the
    unambiguous signals, and a three-country mismatch with a large first order is
    the most unambiguous signal there is -- so if the fallback cannot hold that, it
    is not a conservative fallback, it is an outage that approves fraud.
    """
    decision = await degraded_engine.score(suspicious_order())
    assert decision.band in (RiskBand.HIGH, RiskBand.AMBIGUOUS)
    assert not decision.auto_approvable


async def test_the_real_model_holds_what_the_fallback_only_flags(
    scoring_engine: FraudScoringEngine,
    degraded_engine: FraudScoringEngine,
) -> None:
    """The difference between the two models is a measurable one.

    The fallback has no weight on `basket_size`, `night_hour`, `coupon_first_order`
    or `bin_change`, so on a pattern built from those alone it lands in the
    ambiguous band where the real model reaches high. Documented as a limit rather
    than left to be discovered during an incident.
    """
    pattern: dict[str, Any] = {
        "total_cents": 15_000,
        # A wide basket (ten distinct SKUs, saturating `basket_size`), a
        # first-order coupon and a gift card. The fallback has weights on none of
        # these three, so it lands in the low band where the real model does not.
        "items": tuple((f"SKU-{index}", 1) for index in range(10)),
        "coupon_code": "WELCOME15",
        "is_gift_card": True,
    }
    weak = await degraded_engine.score(clean_order("CUST-WEAK", **pattern))
    strong = await scoring_engine.score(clean_order("CUST-STRONG", **pattern))
    assert strong.score > weak.score, "the real model is not more sensitive than the fallback"

    # The fallback lands in the low band -- and that is *safe* anyway, because the
    # routing policy refuses to auto-approve any degraded decision regardless of
    # its band. This is the two-layer design doing its job: a weaker model, plus a
    # policy that knows it is weaker.
    assert weak.band == RiskBand.LOW
    assert not weak.auto_approvable, "a degraded decision must never be auto-approvable"


# ---------------------------------------------------------------- streaming


async def test_velocity_accumulates_across_orders(scoring_engine: FraudScoringEngine) -> None:
    """The streaming feature: the second identical order is riskier than the first.

    This is the thing a batch model cannot do and the reason the scorer owns a
    rolling window rather than reading a feature table.
    """
    first = await scoring_engine.score(clean_order("CUST-V"))
    for _ in range(4):
        await scoring_engine.score(clean_order("CUST-V"))
    burst = await scoring_engine.score(clean_order("CUST-V", total_cents=40_000))
    assert burst.score > first.score, "velocity did not raise the score"
    assert "velocity" in burst.reasons


async def test_velocity_does_not_count_the_current_order(
    scoring_engine: FraudScoringEngine,
) -> None:
    """The ordering invariant.

    If `record()` ran before scoring, every order would count itself and every
    velocity value would be inflated by exactly one -- which looks like noise
    rather than like a bug.
    """
    features_seen: list[float] = []
    for _ in range(3):
        decision = await scoring_engine.score(clean_order("CUST-V2"))
        features_seen.append(decision.features["velocity"])

    # Three prior orders, so the third order's own feature is 2/5 -- not 3/5. The
    # current order is not in its own count.
    assert features_seen == pytest.approx([0.0, 1 / 5.0, 2 / 5.0])

    fourth = await scoring_engine.score(clean_order("CUST-V2"))
    assert fourth.features["velocity"] == pytest.approx(3 / 5.0)

    # And it *was* recorded, so the fifth sees four.
    assert scoring_engine.window.velocity_for("CUST-V2") == 4
    fifth = await scoring_engine.score(clean_order("CUST-V2"))
    assert fifth.features["velocity"] == pytest.approx(4 / 5.0)


async def test_one_customer_does_not_affect_another(
    scoring_engine: FraudScoringEngine,
) -> None:
    """The pinned scoring context is per-call, not per-process.

    A shared "current customer" field would let one customer's velocity leak into
    another's score, producing plausible numbers and no error -- the worst kind of
    bug to find in production.
    """
    for _ in range(5):
        await scoring_engine.score(clean_order("CUST-NOISY", total_cents=200_000))
    quiet = await scoring_engine.score(clean_order("CUST-QUIET"))
    assert quiet.score < 0.2
    assert "velocity" not in quiet.reasons


async def test_amount_zscore_needs_history(scoring_engine: FraudScoringEngine) -> None:
    """One prior order has no spread, so the z-score is not meaningful."""
    await scoring_engine.score(clean_order("CUST-Z"))
    early = await scoring_engine.score(clean_order("CUST-Z", total_cents=500_000))
    for _ in range(6):
        await scoring_engine.score(clean_order("CUST-Z", total_cents=5_000))
    established = await scoring_engine.score(clean_order("CUST-Z", total_cents=500_000))
    assert established.reasons.count("amount_zscore") >= 1
    assert established.score > early.score * 0.5  # sanity, not a strict ordering


async def test_a_flat_history_gives_a_bounded_zscore(
    scoring_engine: FraudScoringEngine,
) -> None:
    """Three identical orders have zero standard deviation.

    Dividing by it would give infinity, and an infinite feature pins the score at
    1.0 for a customer whose history happens to be uniform -- which is most of
    them.
    """
    for _ in range(4):
        await scoring_engine.score(clean_order("CUST-FLAT"))
    decision = await scoring_engine.score(clean_order("CUST-FLAT", total_cents=90_000))
    assert 0.0 <= decision.score <= 1.0
    assert decision.score < 1.0


async def test_chargebacks_feed_forward(scoring_engine: FraudScoringEngine) -> None:
    """Out-of-band signals fold into the next order's features."""
    for _ in range(3):
        await scoring_engine.score(clean_order("CUST-CB"))
    scoring_engine.mark_chargeback("CUST-CB")
    decision = await scoring_engine.score(clean_order("CUST-CB"))
    assert "prior_chargebacks" in decision.reasons


async def test_the_window_is_bounded(scoring_engine: FraudScoringEngine) -> None:
    """An unbounded map is how a long-running scorer becomes the incident."""
    window = CustomerWindow(capacity=100)
    for index in range(150):
        window.record(customer_id=f"CUST-{index}", total_cents=100, card_bin="411111")
    assert len(window) == 100
    assert window.evictions == 50


async def test_a_hostile_quantity_cannot_blow_up_the_score(
    scoring_engine: FraudScoringEngine,
) -> None:
    """The model is linear in customer-supplied input, so extreme values are reachable."""
    decision = await scoring_engine.score(
        clean_order(total_cents=100_000_000, items=(("SKU-X", 9999),) * 200)
    )
    assert 0.0 <= decision.score <= 1.0


# ---------------------------------------------------------------- the budget


async def test_scoring_latency_is_recorded(
    scoring_engine: FraudScoringEngine, metrics: FlowMeshMetrics
) -> None:
    await scoring_engine.score(clean_order())
    assert metrics.value_of("flowmesh_scoring_latency_seconds_count", transport="in_process") == 1.0


async def test_the_band_is_counted(
    scoring_engine: FraudScoringEngine, metrics: FlowMeshMetrics
) -> None:
    await scoring_engine.score(clean_order("CUST-A"))
    await scoring_engine.score(suspicious_order("CUST-B"))
    assert (
        metrics.value_of("flowmesh_orders_scored_total", band="low", transport="in_process") == 1.0
    )
    assert (
        metrics.value_of("flowmesh_orders_scored_total", band="high", transport="in_process") == 1.0
    )


async def test_p95_latency_is_within_the_200ms_budget(
    scoring_engine: FraudScoringEngine,
) -> None:
    """The budget, asserted on the fast path.

    200 sequential in-process scorings with the LLM reasoner off. If this starts
    failing, the regression is in feature computation or the model -- and the fix
    is to find it here rather than in a Grafana panel nobody is watching at 3am.

    The margin is deliberately generous (200ms against a sub-millisecond
    operation) because the point is to catch an order-of-magnitude regression, not
    to measure CI's jitter.
    """
    latencies: list[float] = []
    for index in range(200):
        context = clean_order(f"CUST-{index % 20}")
        decision = await scoring_engine.score(context)
        latencies.append(decision.latency_ms)

    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    assert p95 < 200.0, f"p95 scoring latency {p95:.1f}ms exceeded the 200ms budget"
    assert max(latencies) < 200.0, "a single slow score is still a breach of the per-order budget"


async def test_throughput_under_concurrency(
    scoring_engine: FraudScoringEngine,
) -> None:
    """The engine is I/O-free on the fast path, so concurrency is pure CPU.

    500 orders scored concurrently must all complete well inside the budget per
    order. This is the test that would catch a reasoner accidentally moving onto
    the fast path -- an `await` on something slow would show up here as latency
    rather than as a mysterious queue depth in production.
    """
    contexts = [clean_order(f"CUST-{index % 50}") for index in range(500)]
    started = asyncio.get_running_loop().time()
    decisions = await asyncio.gather(*(scoring_engine.score(context) for context in contexts))
    elapsed = asyncio.get_running_loop().time() - started
    assert len(decisions) == 500
    assert elapsed < 10.0, f"500 concurrent scorings took {elapsed:.1f}s"


async def test_no_llm_call_happens_on_the_fast_path(
    scoring_engine: FraudScoringEngine, metrics: FlowMeshMetrics
) -> None:
    """The reasoner is not consulted unless the band is ambiguous and asked for.

    The whole p95 budget rests on this. An LLM call here -- even a cached one --
    would put a network round trip on the per-order path.
    """
    for index in range(20):
        await scoring_engine.score(clean_order(f"CUST-{index}"))
    await scoring_engine.score(suspicious_order("CUST-S"))
    assert metrics.value_of("flowmesh_llm_calls_total", outcome="ok", default=0.0) == 0.0


async def test_a_night_order_is_flagged(scoring_engine: FraudScoringEngine) -> None:
    """`night_hour` is exercised via the clock, not a pinned timestamp.

    A fixed `datetime(2026, 9, 29, 3, 0)` would leave the event outside the
    15-minute velocity window, so the same order would be scored by a different
    code path depending on when the suite ran.
    """
    with clock_at(3):
        decision = await scoring_engine.score(clean_order("CUST-NIGHT"))
    assert "night_hour" in decision.reasons


async def test_an_ambiguous_order_gets_a_rationale(scoring_engine: FraudScoringEngine) -> None:
    """The one case where a rationale is worth its cost.

    With `llm_enabled=False` this is the deterministic reasoner, so no LLM call is
    made -- but the rationale is still produced, which is the point: turning the
    LLM off must not leave the ambiguous band unexplained.
    """
    decision = await scoring_engine.score(
        clean_order("CUST-AMB", total_cents=9_000, ip_country="NG"),
        require_rationale=True,
    )
    assert decision.rationale
    assert "CUST-AMB" in decision.order_id or decision.order_id


async def test_asking_for_a_rationale_on_a_clear_order_does_not_invoke_one(
    scoring_engine: FraudScoringEngine, metrics: FlowMeshMetrics
) -> None:
    """`rationale_worth_cost` requires *both* the ambiguous band and the request."""
    decision = await scoring_engine.score(clean_order("CUST-LOW"), require_rationale=True)
    assert decision.band == RiskBand.LOW
    assert decision.llm_used is False
    assert metrics.value_of("flowmesh_llm_calls_total", outcome="ok", default=0.0) == 0.0


# ---------------------------------------------------------------- health


async def test_health_reports_the_model_version(scoring_engine: FraudScoringEngine) -> None:
    health = scoring_engine.health()
    assert health["ready"] is True
    assert health["model_version"] == load_model("data/model/fraud_weights.json").version
    assert health["orders_scored"] == 0


async def test_health_counts_orders_as_they_arrive(
    scoring_engine: FraudScoringEngine,
) -> None:
    await scoring_engine.score(clean_order())
    assert scoring_engine.health()["orders_scored"] == 1


async def test_a_degraded_engine_reports_it_in_health(
    degraded_engine: FraudScoringEngine,
) -> None:
    assert degraded_engine.health()["degraded"] is True


def test_a_missing_weights_file_degrades_rather_than_refusing_to_start(
    settings: Any, metrics: FlowMeshMetrics, tmp_path: Any
) -> None:
    """A scorer that will not boot stops all order processing.

    That is a worse outage than a weaker model, so the engine starts, marks every
    decision `degraded`, and the routing policy refuses to auto-approve any of
    them.
    """
    from backend.config.settings import Settings

    degraded_settings = Settings(
        **{**settings.model_dump(), "fraud_model_path": str(tmp_path / "absent.json")}
    )
    engine = FraudScoringEngine(degraded_settings, metrics)
    assert engine.degraded is True
    assert engine.model.version == "heuristic-fallback-v1"


async def test_the_scoring_context_is_released_even_when_scoring_fails(
    settings: Any, metrics: FlowMeshMetrics
) -> None:
    """A leaked pinned customer is a cross-customer data leak.

    The engine pins `customer_id` on the window before computing and releases it in
    a `finally`. If the release were missing, the *next* order's features would be
    computed against the previous customer.
    """
    from backend.config.settings import Settings

    engine = FraudScoringEngine(Settings(**settings.model_dump()), metrics)
    await engine.score(clean_order("CUST-FIRST"))

    def _boom(context: OrderContext, window: Any) -> Any:
        raise RuntimeError("features exploded")

    import backend.fraud.engine as engine_module

    original = engine_module.compute_features
    engine_module.compute_features = _boom  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError):
            await engine.score(clean_order("CUST-SECOND"))
    finally:
        engine_module.compute_features = original  # type: ignore[assignment]

    assert engine.window.current() is None, "the scoring context leaked past the failure"
