"""Risk band classification.

The band *is* the policy, so it gets the most exhaustive test in the project: a
sweep across the whole score domain plus the exact boundaries. A bug here does
not throw -- it silently approves an order that should have been reviewed, which
is the failure nobody notices until it costs money.
"""

from __future__ import annotations

import pytest

from backend.events.schema import RiskBand
from backend.fraud.bands import (
    REASON_LABELS,
    band_for,
    is_reviewable,
    rationale_worth_cost,
)

LOW = 0.35
HIGH = 0.65


def test_below_low_is_low() -> None:
    assert band_for(0.0, LOW, HIGH) == RiskBand.LOW
    assert band_for(0.3499, LOW, HIGH) == RiskBand.LOW


def test_at_low_threshold_is_ambiguous() -> None:
    # The edge belongs to the *reviewable* side. A customer exactly on a
    # threshold should be looked at, not approved.
    assert band_for(0.35, LOW, HIGH) == RiskBand.AMBIGUOUS


def test_at_high_threshold_is_high() -> None:
    assert band_for(0.65, LOW, HIGH) == RiskBand.HIGH


def test_inside_band_is_ambiguous() -> None:
    assert band_for(0.5, LOW, HIGH) == RiskBand.AMBIGUOUS


def test_boundaries_are_half_open() -> None:
    """`[low, high)` -- documented, so the edges are pinned."""
    assert band_for(LOW - 1e-9, LOW, HIGH) == RiskBand.LOW
    assert band_for(LOW, LOW, HIGH) == RiskBand.AMBIGUOUS
    assert band_for(HIGH - 1e-9, LOW, HIGH) == RiskBand.AMBIGUOUS
    assert band_for(HIGH, LOW, HIGH) == RiskBand.HIGH
    assert band_for(1.0, LOW, HIGH) == RiskBand.HIGH


@pytest.mark.parametrize(
    "score,expected",
    [
        (0.00, RiskBand.LOW),
        (0.10, RiskBand.LOW),
        (0.34, RiskBand.LOW),
        (0.35, RiskBand.AMBIGUOUS),
        (0.45, RiskBand.AMBIGUOUS),
        (0.64, RiskBand.AMBIGUOUS),
        (0.65, RiskBand.HIGH),
        (0.80, RiskBand.HIGH),
        (1.00, RiskBand.HIGH),
    ],
)
def test_sweep(score: float, expected: RiskBand) -> None:
    assert band_for(score, LOW, HIGH) == expected


def test_every_score_in_the_domain_classifies() -> None:
    """No score in [0, 1] may fall outside the three bands.

    The exhaustive version of the sweep: a future edit that adds a fourth band
    without covering the edges shows up here rather than in production.
    """
    seen: set[RiskBand] = set()
    for step in range(0, 1001):
        score = step / 1000
        band = band_for(score, LOW, HIGH)
        seen.add(band)
    assert seen == {RiskBand.LOW, RiskBand.AMBIGUOUS, RiskBand.HIGH}


def test_rejects_crossed_thresholds() -> None:
    with pytest.raises(ValueError, match="thresholds"):
        band_for(0.5, 0.7, 0.3)


def test_rejects_out_of_range_thresholds() -> None:
    with pytest.raises(ValueError, match="thresholds"):
        band_for(0.5, -0.1, 0.9)
    with pytest.raises(ValueError, match="thresholds"):
        band_for(0.5, 0.3, 1.5)


def test_rejects_out_of_range_score() -> None:
    with pytest.raises(ValueError, match="score must be"):
        band_for(1.5, LOW, HIGH)
    with pytest.raises(ValueError, match="score must be"):
        band_for(-0.01, LOW, HIGH)


def test_only_ambiguous_and_high_are_reviewable() -> None:
    assert not is_reviewable(RiskBand.LOW)
    assert is_reviewable(RiskBand.AMBIGUOUS)
    assert is_reviewable(RiskBand.HIGH)


def test_rationale_costs_money_only_on_the_ambiguous_band() -> None:
    """Two conditions: the band, and a caller that asked for it.

    This pair is the p95 budget. The fast path sets `require_rationale=False`,
    so no order on it can ever trigger an LLM call.
    """
    assert rationale_worth_cost(RiskBand.AMBIGUOUS, requested=True)
    # Not requested: nobody is going to read it.
    assert not rationale_worth_cost(RiskBand.AMBIGUOUS, requested=False)
    # Requested, but the model is sure -- no call.
    assert not rationale_worth_cost(RiskBand.LOW, requested=True)
    assert not rationale_worth_cost(RiskBand.HIGH, requested=True)


def test_every_feature_has_a_human_label() -> None:
    """A reason code with no label is a column an operator learns to ignore."""
    from backend.fraud.features import FEATURE_NAMES

    missing = [name for name in FEATURE_NAMES if name not in REASON_LABELS]
    assert not missing, f"features with no operator-facing label: {missing}"


def test_labels_are_sentences_not_identifiers() -> None:
    for name, label in REASON_LABELS.items():
        assert name in label or " " in label, f"label for {name} is not readable: {label!r}"
        assert label != name
