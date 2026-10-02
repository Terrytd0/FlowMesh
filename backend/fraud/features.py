"""Feature computation: turning one order plus recent history into a vector.

Every feature is scaled into roughly [0, 1] *before* it meets the model, which
is the whole reason the model's weights in `data/model/fraud_weights.json` are
readable numbers rather than magic. A weight of `1.6` on `country_mismatch`
means "a billing/shipping country split is worth 1.6 logits", and a reviewer
can argue with it.

The three-country feature set (`card_country`, `billing_country`,
`shipping_country`, `ip_country`) is deliberately three separate features rather
than one "is it weird" flag, because the weights differ and fraud-ops needs to
know *which* mismatch triggered a hold. Collapsing them would also make the model
unlearnable from its own contributions.

`basket_size` and `velocity` are saturating, not linear: the difference between
0 and 3 recent orders matters, the difference between 20 and 23 does not. Linear
features on a saturating quantity are how a model learns to spend all its
confidence on a single extreme customer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from backend.core.clock import utcnow
from backend.fraud.profile_window import CustomerWindow

#: Distinct SKUs at which the basket-size feature saturates.
_BASKET_SATURATION = 8.0

#: UTC hours treated as "the small hours" for the delivery country.
_NIGHT_HOURS = frozenset({0, 1, 2, 3, 4, 5})

#: Feature order. Fixed, and the order the weights file is validated against.
FEATURE_NAMES: tuple[str, ...] = (
    "amount_zscore",
    "velocity",
    "basket_size",
    "country_mismatch",
    "card_country_mismatch",
    "ip_country_mismatch",
    "first_order",
    "night_hour",
    "coupon_first_order",
    "gift_card",
    "prior_chargebacks",
    "bin_change",
)


@dataclass(frozen=True)
class OrderContext:
    """Everything the scorer is allowed to see about an order.

    Assembled by the gRPC layer from the protobuf request, so it is a plain
    value object with no transport types in it -- the same object is built from
    an `EventEnvelope` in the in-process path, which is what keeps the two
    transports from drifting into two different feature sets.
    """

    order_id: str
    customer_id: str
    total_cents: int
    items: tuple[tuple[str, int], ...]
    card_bin: str
    card_country: str
    billing_country: str
    shipping_country: str
    ip_country: str
    coupon_code: str | None = None
    is_gift_card: bool = False
    received_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class FeatureVector:
    """A named vector, plus the customer state it was derived from.

    `as_dict` preserves `FEATURE_NAMES` order, and the weights file is validated
    against that order at load -- a reordered JSON object would otherwise produce
    a model that scores plausible numbers using the wrong weights, and nothing
    would ever fail.
    """

    values: dict[str, float]

    def as_dict(self) -> dict[str, float]:
        return {name: self.values.get(name, 0.0) for name in FEATURE_NAMES}

    def __getitem__(self, name: str) -> float:
        return self.values.get(name, 0.0)

    def vector(self) -> list[float]:
        return [self.values.get(name, 0.0) for name in FEATURE_NAMES]


def compute_features(context: OrderContext, window: CustomerWindow) -> FeatureVector:
    """Compute the feature vector for one order.

    Reads the window for `context.customer_id`; the caller is responsible for
    pinning the scoring context (see `CustomerWindow.scoring_context`) and for
    releasing it afterwards.
    """
    distinct_skus = len({sku for sku, _ in context.items})
    total_skus = sum(quantity for _, quantity in context.items)

    history_bins = window.known_bins()
    order_count = window.order_count()

    return FeatureVector(
        values={
            # History-derived: how far above the customer's own average.
            "amount_zscore": window.amount_zscore(context.total_cents) / 4.0,
            # Saturation, and it is orders-per-window rather than per-second so
            # the threshold means something a human can read ("five orders in
            # fifteen minutes").
            "velocity": window.velocity_feature(context.received_at),
            "basket_size": min(1.0, distinct_skus / _BASKET_SATURATION) * 0.5
            + min(1.0, total_skus / (_BASKET_SATURATION * 2)) * 0.5,
            # Three independent mismatches, three weights. Billing vs shipping
            # is the classic drop-ship pattern; card vs shipping is the classic
            # stolen-card pattern; IP is the classic botnet pattern.
            "country_mismatch": _mismatch(context.billing_country, context.shipping_country),
            "card_country_mismatch": _mismatch(context.card_country, context.shipping_country),
            "ip_country_mismatch": _mismatch(context.ip_country, context.shipping_country),
            "first_order": 1.0 if order_count == 0 else 0.0,
            "night_hour": 1.0 if context.received_at.hour in _NIGHT_HOURS else 0.0,
            "coupon_first_order": 1.0 if (context.coupon_code and order_count == 0) else 0.0,
            "gift_card": 1.0 if context.is_gift_card else 0.0,
            "prior_chargebacks": min(1.0, window.chargeback_rate() * 4.0),
            "bin_change": 1.0 if (history_bins and context.card_bin not in history_bins) else 0.0,
        }
    )


def _mismatch(left: str, right: str) -> float:
    """1.0 when two countries differ, 0.0 when they match or either is unknown.

    Unknown is 0.0 rather than 1.0 on purpose: a missing country code is missing
    *data*, and treating missing data as evidence of fraud would hold every order
    from a customer whose profile happens not to carry a country. The cost of
    that mistake is a review queue full of nothing.
    """
    if not left or not right:
        return 0.0
    return 0.0 if left.strip().lower() == right.strip().lower() else 1.0


def features_for_context(
    customer_id: str, total_cents: int, context: OrderContext, window: CustomerWindow
) -> FeatureVector:
    """Pinned-context convenience wrapper used by the in-process transport."""
    window.scoring_context(customer_id)
    try:
        return compute_features(context, window)
    finally:
        window.release_context()


def top_features(vector: FeatureVector, limit: int = 5) -> list[tuple[str, float]]:
    """The largest features, for a dashboard or a log line."""
    ordered = sorted(vector.as_dict().items(), key=lambda pair: pair[1], reverse=True)
    return [(name, value) for name, value in ordered[:limit] if value > 0.0]


def describe_features(values: dict[str, float]) -> str:
    """One-line summary of a vector, for structured logging."""
    parts = [f"{name}={value:.2f}" for name, value in values.items() if value > 0.0]
    return " ".join(parts) if parts else "no elevated features"


def as_any_dict(vector: FeatureVector) -> dict[str, Any]:
    """For the protobuf map field, which wants string keys."""
    return {name: float(value) for name, value in vector.as_dict().items()}
