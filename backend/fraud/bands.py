"""Risk bands: the boundary between "ship it" and "a human looks at this".

One function owns the boundary, and everything else asks it. The thresholds live
in settings (`fraud_low_threshold`, `fraud_high_threshold`), never inline, because
an inline threshold is a threshold that gets tuned in one place and not the
other -- and then the low-risk path and the dashboard disagree about which
orders are held.

The edges are half-open, `[low, high)`, and a score of exactly 0.65 is *high*
while 0.349999 is *ambiguous*. That asymmetry is deliberate: the ambiguous band
is the one that costs an LLM call, and a customer sitting exactly on a threshold
should be reviewed rather than approved.

A "band" is not a severity, and the distinction is worth stating because the
words get used interchangeably in conversation: `low` means *nothing here needs a
person*, `ambiguous` means *the model is unsure and a reasoner should try*,
`high` means *the model's strong signal is that something is wrong*. Only the
last two are fraud allegations, and the pipeline treats them differently: the
high band gets a human regardless of what the reasoner says.
"""

from __future__ import annotations

from backend.events.schema import RiskBand


def band_for(score: float, low: float, high: float) -> RiskBand:
    """Classify a score against explicit thresholds.

    Thresholds are parameters, not settings, so the function is pure and the
    boundary logic can be tested without a settings object or an event loop.
    """
    if not 0.0 <= low < high <= 1.0:
        raise ValueError(f"thresholds must satisfy 0 <= low < high <= 1, got low={low} high={high}")
    if score < 0.0 or score > 1.0:
        raise ValueError(f"score must be in [0, 1], got {score}")
    if score < low:
        return RiskBand.LOW
    if score < high:
        return RiskBand.AMBIGUOUS
    return RiskBand.HIGH


def is_reviewable(band: RiskBand) -> bool:
    """True when a human (or a reasoner) should look at this order."""
    return band in (RiskBand.AMBIGUOUS, RiskBand.HIGH)


def rationale_worth_cost(band: RiskBand, requested: bool) -> bool:
    """Whether to spend an LLM call explaining this decision.

    Both conditions, and the second is the one people forget: a rationale is
    only worth its latency if somebody is going to read it. The fast path sets
    `require_rationale=False` and therefore never triggers a call, which is what
    keeps the p95 inside 200ms.
    """
    return bool(requested) and band == RiskBand.AMBIGUOUS


#: Labels used in the generated rationale, keyed by feature name. Kept next to
#: the band logic because a reason code with no human-readable label is a column
#: an operator learns to ignore.
REASON_LABELS: dict[str, str] = {
    "amount_zscore": "order value far above this customer's history",
    "velocity": "unusual number of recent orders from this customer",
    "basket_size": "unusually wide basket",
    "country_mismatch": "billing and shipping countries differ",
    "card_country_mismatch": "card issued in a different country to the delivery",
    "ip_country_mismatch": "order placed from a third country",
    "first_order": "no order history for this customer",
    "night_hour": "order placed in the small hours in the delivery timezone",
    "coupon_first_order": "first order redeemed a discount code",
    "gift_card": "gift-card order",
    "prior_chargebacks": "customer has a history of chargebacks",
    "bin_change": "card BIN differs from this customer's previous orders",
}
