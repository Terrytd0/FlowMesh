"""Routing policy: what happens to a scored order.

One function, one decision, three outcomes. It lives in its own module -- not in
the order processor, not in the API -- because it is the single most consequential
piece of policy in the project and the piece a fraud-ops team will want to argue
with. Making it a pure function of `(score, band, degraded)` means it can be
exhaustively tested over the whole score range without a database, a broker or a
running pipeline, and the review dashboard can ask "what would this order have
done?" without replaying it.

Two rules that are policy rather than mechanics, and both are deliberately
conservative:

**1. A degraded score never auto-approves.** If the real model could not be
loaded and a fallback heuristic decided, the order is held. The reasoning: the
fallback is a weaker model, so its low scores are less trustworthy, and an order
approved on an untrustworthy low score is the failure that costs money. The cost
of this rule is a review queue that fills during a model outage -- which is a
cost you can see, and the right thing to see.

**2. The high band goes to a human no matter what the reasoner said.** An LLM
explanation attached to a high score cannot overturn it. If it could, the score
would stop being the decision and the prose would be, and the prose is the part
of the system with no audit trail worth the name.

`RouteDecision` says what to do and why, because the `why` is what goes in the
audit log and what an operator reads at 3am.
"""

from __future__ import annotations

from dataclasses import dataclass

from backend.events.schema import RiskBand


@dataclass(frozen=True)
class RouteDecision:
    """Where a scored order goes, and the sentence explaining why."""

    #: `"approve"` | `"review"` -- the two legal outcomes.
    action: str
    reason: str
    #: Whether this route requires an LLM rationale to be attached.
    requires_rationale: bool = False

    @property
    def needs_review(self) -> bool:
        return self.action == "review"


APPROVE = "approve"
REVIEW = "review"


def route_for(
    *,
    score: float,
    band: RiskBand,
    degraded: bool = False,
    low_threshold: float = 0.35,
    high_threshold: float = 0.65,
) -> RouteDecision:
    """Decide what happens to a scored order.

    `low_threshold`/`high_threshold` are passed rather than read from settings so
    this stays a pure function; `band` is passed too, and the consistency between
    them is asserted, because a caller that computed a band with one threshold and
    routed with another would produce a system whose dashboard disagrees with its
    own policy.
    """
    if not 0.0 <= low_threshold < high_threshold <= 1.0:
        raise ValueError(
            "thresholds must satisfy 0 <= low < high <= 1, got "
            f"low={low_threshold} high={high_threshold}"
        )
    expected = _expected_band(score, low_threshold, high_threshold)
    if expected is not band:
        raise ValueError(
            f"band {band!r} does not match score {score} at thresholds "
            f"({low_threshold}, {high_threshold}); expected {expected!r}"
        )

    if degraded:
        # Rule 1. Deliberately before the band check.
        return RouteDecision(
            action=REVIEW,
            reason=(
                f"scored {score:.2f} by the fallback model ({band}); a degraded "
                "score is never auto-approved"
            ),
        )

    if band == RiskBand.LOW:
        return RouteDecision(
            action=APPROVE, reason=f"scored {score:.2f}, below {low_threshold:.2f}"
        )

    if band == RiskBand.HIGH:
        # Rule 2.
        return RouteDecision(
            action=REVIEW,
            reason=f"scored {score:.2f}, at or above {high_threshold:.2f}",
        )

    return RouteDecision(
        action=REVIEW,
        reason=f"scored {score:.2f}, inside the ambiguous band",
        requires_rationale=True,
    )


def _expected_band(score: float, low: float, high: float) -> RiskBand:
    if score < low:
        return RiskBand.LOW
    if score < high:
        return RiskBand.AMBIGUOUS
    return RiskBand.HIGH
