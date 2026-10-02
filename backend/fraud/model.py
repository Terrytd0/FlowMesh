"""The model: logistic regression over the feature vector.

Deliberately the simplest model that can be argued with in a meeting. The point
of this sprint is the streaming pipeline around the decision -- the event log,
the task queue, the service boundary, the metrics -- not the classifier. A
gradient-boosted tree would score marginally better and be dramatically worse at
the thing that actually matters here: a fraud analyst asking "why was this order
held?" and getting an answer that is a number they can look up.

That answer is `contributions()`. Every feature contributes `weight * value` to
the logit, the contributions sum exactly to `logit - intercept`, and the reason
codes are ordered by contribution. So the response can say *what* drove the score
and by how much, and a human can disagree with the weight.

Weights live in a JSON file, loaded at startup, validated against
`FEATURE_NAMES`. The validation is the load-bearing part: a weights file with a
missing or misspelled feature would otherwise load happily and score every order
with a silently zeroed input.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.core.logging import get_logger
from backend.fraud.bands import REASON_LABELS
from backend.fraud.features import FEATURE_NAMES, FeatureVector

logger = get_logger(__name__)

#: A contribution below this is not worth telling an operator about. At a logit
#: scale of roughly -4 to +5, 0.15 is the point where a feature has moved the
#: score enough to matter and the reason list has not become a wall of text.
#:
#: It is 0.18 rather than 0.15 for one specific reason: a customer with a single
#: prior order has `velocity = 1/5 = 0.2`, which at a weight of 0.85 contributes
#: 0.17 -- and "unusual number of recent orders" on a customer with *one* recent
#: order is not a finding, it is the baseline. At 0.15 every returning customer
#: carries a reason code that means nothing, and a reason list that is always
#: populated is a reason list nobody reads. The genuinely elevated velocity case
#: (4+ orders in the window) contributes 0.68 and reports comfortably.
REASON_CONTRIBUTION_FLOOR = 0.18

#: Reasons beyond this many are dropped. The top contributors plus a count is
#: more useful than twelve bullets, and an unbounded reason list is a payload
#: that grows with the feature count.
MAX_REASONS = 5


@dataclass(frozen=True)
class Contribution:
    """One feature's push on the logit."""

    feature: str
    value: float
    weight: float

    @property
    def contribution(self) -> float:
        return self.weight * self.value

    @property
    def label(self) -> str:
        return REASON_LABELS.get(self.feature, self.feature)


@dataclass(frozen=True)
class FraudModel:
    """An immutable, loaded logistic model."""

    version: str
    intercept: float
    weights: dict[str, float]
    #: Free-text provenance, surfaced in the health response and the README.
    trained_at: str = ""
    notes: str = ""

    def score(self, vector: FeatureVector) -> float:
        """Calibrated probability of fraud, in [0, 1]."""
        logit = self.intercept + self._weighted_sum(vector)
        return _sigmoid(logit)

    def logit(self, vector: FeatureVector) -> float:
        return self.intercept + self._weighted_sum(vector)

    def contributions(self, vector: FeatureVector) -> list[Contribution]:
        """Per-feature contributions, largest first."""
        values = vector.as_dict()
        rows = [
            Contribution(feature=name, value=values[name], weight=self.weights.get(name, 0.0))
            for name in FEATURE_NAMES
        ]
        return sorted(rows, key=lambda row: row.contribution, reverse=True)

    def reasons(self, vector: FeatureVector, *, limit: int = MAX_REASONS) -> list[str]:
        """Reason codes worth surfacing, largest contribution first."""
        rows = [
            row
            for row in self.contributions(vector)
            if row.contribution >= REASON_CONTRIBUTION_FLOOR and row.value > 0.0
        ]
        return [row.feature for row in rows[:limit]]

    def _weighted_sum(self, vector: FeatureVector) -> float:
        values = vector.as_dict()
        return sum(self.weights.get(name, 0.0) * values[name] for name in FEATURE_NAMES)

    def explain(self, vector: FeatureVector) -> dict[str, Any]:
        """The full breakdown, for the dashboard's "why this score" panel."""
        return {
            "version": self.version,
            "intercept": self.intercept,
            "logit": self.logit(vector),
            "score": self.score(vector),
            "contributions": [
                {
                    "feature": row.feature,
                    "label": row.label,
                    "value": row.value,
                    "weight": row.weight,
                    "contribution": row.contribution,
                }
                for row in self.contributions(vector)
            ],
        }


def _sigmoid(value: float) -> float:
    """Numerically stable logistic.

    The naive `1 / (1 + exp(-x))` overflows for `x < -709`, and `exp(709)`
    overflows for large positive x. Since the model is a linear function of
    adversarial input, "large" is reachable by a customer who tries, and the
    failure mode is an `OverflowError` inside the scoring path -- one request
    taking down the consumer. Two branches, no overflow.
    """
    if value >= 0.0:
        return 1.0 / (1.0 + math.exp(-value))
    exponential = math.exp(value)
    return exponential / (1.0 + exponential)


def load_model(path: str | Path) -> FraudModel:
    """Load and validate a weights file.

    Raises `ValueError` on a missing feature rather than defaulting it to 0.0. A
    typo in a weight name is invisible in production -- the model keeps scoring,
    it just quietly ignores a signal -- so it is made fatal here, once, at
    startup.
    """
    file_path = Path(path)
    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"model weights not found: {file_path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"model weights at {file_path} are not valid JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise ValueError(f"model weights at {file_path} must be a JSON object")

    weights = raw.get("weights")
    if not isinstance(weights, dict):
        raise ValueError(f"model weights at {file_path} must contain a 'weights' object")

    missing = [name for name in FEATURE_NAMES if name not in weights]
    if missing:
        raise ValueError(
            f"model weights at {file_path} are missing features {missing}; "
            f"every one of {list(FEATURE_NAMES)} must be present"
        )
    unknown = [name for name in weights if name not in FEATURE_NAMES]
    if unknown:
        # Not fatal: a weights file may carry features this build has retired.
        # Logged, because "why is this weight ignored" is a real question and
        # the answer belongs in the log rather than in someone's memory.
        logger.warning("model weights contain unknown features, ignoring: %s", unknown)

    intercept = raw.get("intercept")
    if not isinstance(intercept, (int, float)):
        raise ValueError(f"model weights at {file_path} must contain a numeric 'intercept'")

    return FraudModel(
        version=str(raw.get("version", "unversioned")),
        intercept=float(intercept),
        weights={name: float(weights[name]) for name in FEATURE_NAMES},
        trained_at=str(raw.get("trained_at", "")),
        notes=str(raw.get("notes", "")),
    )


class HeuristicFallback:
    """The model used when the real one cannot be loaded.

    A fraud model that is unavailable must not become a fraud model that approves
    everything, so the fallback is not a zero vector: it is a small, fixed,
    hand-weighted scorer that fires on the unambiguous signals (country
    mismatch, high value relative to nothing, new accounts, velocity). Orders it
    scores into the low band are still only auto-approved if the pipeline's
    degraded policy allows it -- see `backend/pipeline/routing.py`, which refuses
    to auto-approve anything scored by this class.

    Being explicit about this is the point: the alternative, `except: score = 0`,
    is a one-line outage policy that silently stops fraud detection.

    Not a dataclass. It carries no state and has no instances -- `as_model()`
    builds a real `FraudModel`, which is the only thing callers ever hold. Making
    it a class with one method keeps the "there is exactly one fallback" fact
    visible; a dataclass would imply it could be instantiated and configured per
    call site, which is a different design with the same name.
    """

    VERSION = "heuristic-fallback-v1"

    WEIGHTS: dict[str, float] = {
        "amount_zscore": 0.9,
        "velocity": 0.8,
        "country_mismatch": 1.2,
        "card_country_mismatch": 0.9,
        "ip_country_mismatch": 0.6,
        "first_order": 0.5,
        "prior_chargebacks": 0.7,
    }

    def as_model(self) -> FraudModel:
        return FraudModel(
            version=self.VERSION,
            intercept=-2.0,
            weights=dict(self.WEIGHTS),
            notes="conservative fallback used when the trained weights are unavailable",
        )


def load_model_or_fallback(path: str | Path) -> tuple[FraudModel, bool]:
    """Load the model, degrading to the fallback rather than refusing to start.

    Returns `(model, degraded)`. The service starts either way -- a fraud scorer
    that will not boot is a scorer that stops all order processing, which is a
    worse outage than a weaker model -- but every decision it makes is marked
    `degraded`, counted in `flowmesh_scoring_degraded_total`, and routed to
    review rather than auto-approved.
    """
    try:
        return load_model(path), False
    except ValueError as exc:
        logger.error("model unavailable, using conservative fallback: %s", exc)
        return HeuristicFallback().as_model(), True
