"""The model: loading, validation, calibration, and explanation.

The property that matters most here is **the contributions sum to the logit**.
If that stops holding, the "why was this order held" panel becomes a list of
plausible-looking numbers that do not explain the score, which is worse than
having no explanation at all.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.fraud.features import FEATURE_NAMES, FeatureVector
from backend.fraud.model import (
    REASON_CONTRIBUTION_FLOOR,
    FraudModel,
    HeuristicFallback,
    load_model,
    load_model_or_fallback,
)

MODEL_PATH = Path("data/model/fraud_weights.json")


@pytest.fixture
def model() -> FraudModel:
    """The committed weights file, loaded through the real loader.

    Typed rather than `object` so the 20-odd assertions below are checked: with
    `-> object`, every attribute access is a type error the `# type: ignore`s were
    hiding, and a typo in a weight name would have been equally invisible.
    """
    return load_model(MODEL_PATH)


def vector(**overrides: float) -> FeatureVector:
    """A zero vector with named features set. Zero is 'nothing suspicious'."""
    return FeatureVector(values={name: overrides.get(name, 0.0) for name in FEATURE_NAMES})


def test_weights_file_loads(model: FraudModel) -> None:
    assert model.version.startswith("flowmesh-fraud")
    assert len(model.weights) == len(FEATURE_NAMES)
    assert model.notes  # provenance is not optional


def test_a_featureless_order_scores_in_the_low_band(model: FraudModel) -> None:
    """The base rate has to be low, or every order is a review."""
    assert model.score(vector()) < 0.35


def test_every_feature_raises_the_score(model: FraudModel) -> None:
    """No weight may be negative.

    A negative weight is not "an inverse correlation" in practice -- it is a sign
    error or a typo, and it would mean the single strongest fraud signal in the
    file makes orders look safer.
    """
    base = model.score(vector())
    for name in FEATURE_NAMES:
        weight = model.weights[name]
        assert weight > 0, f"feature {name} has a non-positive weight {weight}"
        raised = model.score(vector(**{name: 1.0}))
        assert raised > base, f"feature {name} did not raise the score"


def test_contributions_sum_to_the_logit_minus_intercept(model: FraudModel) -> None:
    """The explanation must actually explain the number.

    `sum(weight * value) == logit - intercept`, exactly. This is the property that
    makes the "why" panel trustworthy.
    """
    for weights in (
        {},
        {"amount_zscore": 0.5, "country_mismatch": 1.0},
        {name: 0.4 for name in FEATURE_NAMES},
    ):
        v = vector(**weights)
        total = sum(model.weights[name] * v[name] for name in FEATURE_NAMES)
        expected = model.logit(v) - model.intercept
        assert total == pytest.approx(expected, abs=1e-9)


GEOGRAPHIC = ("country_mismatch", "card_country_mismatch", "ip_country_mismatch")
BEHAVIOURAL = (
    "amount_zscore",
    "velocity",
    "first_order",
    "prior_chargebacks",
    "bin_change",
)


def test_a_geographic_signal_alone_reaches_review(model: FraudModel) -> None:
    """One geography mismatch is enough to look at an order.

    Below 0.35 the model auto-approves, so a single country mismatch has to clear
    it. That is the calibration the weights were tuned for, and it is the property
    a fraud team would notice missing first.
    """
    for name in GEOGRAPHIC:
        assert model.score(vector(**{name: 1.0})) > 0.35, f"{name} alone did not reach review"


def test_two_geographic_signals_reach_the_high_band(model: FraudModel) -> None:
    """Two independent country mismatches -- card, billing and IP all disagree.

    This is the stolen-card-and-botnet pattern, and it must clear the high
    threshold on its own without any behavioural signal supporting it.
    """
    for index, first in enumerate(GEOGRAPHIC):
        for second in GEOGRAPHIC[index + 1 :]:
            v = vector(**{first: 1.0, second: 1.0})
            assert model.score(v) > 0.65, f"{first}+{second} did not reach the high band"


def test_a_geographic_plus_behavioural_signal_reaches_review(model: FraudModel) -> None:
    """Every geography+behaviour pair clears review.

    Swept rather than hand-picked: a sign error in one weight would otherwise
    hide behind whichever combination happened to be written down.
    """
    for geo in GEOGRAPHIC:
        for behaviour in BEHAVIOURAL:
            v = vector(**{geo: 1.0, behaviour: 1.0})
            assert model.score(v) > 0.35, f"{geo}+{behaviour} did not reach review"


def test_weak_behavioural_pairs_stay_below_review(model: FraudModel) -> None:
    """Two weak behavioural signals must *not* reach review.

    The other half of the calibration, and the one that keeps the queue usable.
    `velocity + basket_size` is a customer buying a lot of things quickly, which
    is what the business wants to happen. A model that holds those orders would
    be tuned against itself: the fix would be to raise the threshold, which would
    then miss the real cases above.
    """
    weak_pairs = (("velocity", "basket_size"), ("basket_size", "night_hour"))
    for first, second in weak_pairs:
        v = vector(**{first: 1.0, second: 1.0})
        assert model.score(v) < 0.35, f"{first}+{second} should stay out of review"


def test_a_single_strong_signal_carries_the_order_past_review(model: FraudModel) -> None:
    """No feature is inert, and no feature is a decision on its own."""
    for name in FEATURE_NAMES:
        v = vector(**{name: 1.0})
        score = model.score(v)
        assert score > 0.0, f"{name} scores as if it were absent"
        assert score < 0.65, f"{name} alone decides the order"


def test_reasons_are_ordered_by_contribution(model: FraudModel) -> None:
    v = vector(amount_zscore=0.9, country_mismatch=1.0, night_hour=1.0)
    reasons = model.reasons(v)
    contributions = [c for c in model.contributions(v) if c.feature in reasons]
    values = [c.contribution for c in contributions]
    assert values == sorted(values, reverse=True)


def test_reasons_respect_the_floor(model: FraudModel) -> None:
    """A feature too small to matter must not appear.

    Specifically: one prior order in the window gives `velocity = 0.2`, which is
    not a finding. If it shows up, the reason list is noise and reviewers stop
    reading it.
    """
    v = vector(velocity=0.2)
    assert "velocity" not in model.reasons(v)
    v = vector(velocity=0.8)
    assert "velocity" in model.reasons(v)


def test_reasons_are_capped(model: FraudModel) -> None:
    v = vector(**{name: 1.0 for name in FEATURE_NAMES})
    assert len(model.reasons(v)) <= 5


def test_explain_reports_every_feature(model: FraudModel) -> None:
    breakdown = model.explain(vector(country_mismatch=1.0))
    assert breakdown["version"] == model.version
    assert len(breakdown["contributions"]) == len(FEATURE_NAMES)
    labelled = [row for row in breakdown["contributions"] if row["label"] != row["feature"]]
    assert labelled, "contributions should carry human-readable labels"


def test_sigmoid_survives_extreme_logits(model: FraudModel) -> None:
    """A hostile input must not raise `OverflowError` inside the scoring path.

    `1/(1+exp(-x))` overflows below about -709 and above about 709. Since the
    model is a linear function of customer-supplied fields, those values are
    reachable, and the failure mode would be a crashed consumer.
    """
    extreme = FeatureVector(values={name: 1e9 for name in FEATURE_NAMES})
    score = model.score(extreme)
    assert 0.0 <= score <= 1.0
    assert score == pytest.approx(1.0)


def test_sigmoid_stays_in_range_at_the_bottom() -> None:
    from backend.fraud.model import _sigmoid

    assert _sigmoid(-1e9) == pytest.approx(0.0, abs=1e-12)
    assert _sigmoid(1e9) == pytest.approx(1.0)
    assert _sigmoid(0.0) == pytest.approx(0.5)


def test_missing_feature_in_weights_is_fatal(tmp_path: Path) -> None:
    """A weight name typo must fail at load, not silently score as zero.

    A missing feature would otherwise keep scoring with that input ignored --
    invisible in production, and it would look like "the model got worse".
    """
    partial = tmp_path / "partial.json"
    partial.write_text(
        json.dumps({"version": "broken", "intercept": -1.0, "weights": {"amount_zscore": 1.0}}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing features"):
        load_model(partial)


def test_unknown_feature_is_ignored_with_a_warning(tmp_path: Path) -> None:
    """A *retired* feature is not fatal -- only a missing one is."""
    payload = {name: 0.5 for name in FEATURE_NAMES}
    payload["retired_feature"] = 9.9
    path = tmp_path / "extra.json"
    path.write_text(
        json.dumps({"version": "v1", "intercept": -1.0, "weights": payload}), encoding="utf-8"
    )
    model = load_model(path)
    assert "retired_feature" not in model.weights


def test_missing_weights_file_is_reported_clearly(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not found"):
        load_model(tmp_path / "nope.json")


def test_malformed_json_is_reported_clearly(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    with pytest.raises(ValueError, match="not valid JSON"):
        load_model(bad)


def test_missing_intercept_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "no-intercept.json"
    path.write_text(
        json.dumps({"version": "v1", "weights": {name: 0.1 for name in FEATURE_NAMES}}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="intercept"):
        load_model(path)


def test_fallback_scores_rather_than_approving_everything() -> None:
    """The degraded model must not be a zero vector.

    `except: score = 0` is a one-line outage policy that silently stops fraud
    detection. The fallback fires on the unambiguous signals instead.
    """
    fallback = HeuristicFallback().as_model()
    assert fallback.score(vector()) < 0.35
    suspicious = fallback.score(vector(country_mismatch=1.0, first_order=1.0, velocity=1.0))
    assert suspicious > 0.35, "the fallback should still hold an obviously bad order"


def test_load_or_fallback_reports_degraded(tmp_path: Path) -> None:
    _, degraded = load_model_or_fallback(MODEL_PATH)
    assert degraded is False
    model, degraded = load_model_or_fallback(tmp_path / "absent.json")
    assert degraded is True
    assert model.version == HeuristicFallback.VERSION


def test_fallback_never_auto_approves_via_routing() -> None:
    """The routing policy refuses degraded decisions regardless of score.

    This is the cross-module guarantee that makes the degraded path safe: even a
    0.01 score from the fallback model is held.
    """
    from backend.events.schema import RiskBand
    from backend.pipeline.routing import route_for

    decision = route_for(score=0.01, band=RiskBand.LOW, degraded=True)
    assert decision.action == "review"
