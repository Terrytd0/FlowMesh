"""The scoring engine: one order in, one decision out.

This is the whole of the fraud service's intelligence, and it is deliberately
transport-free. The same `score()` runs behind a real gRPC server and inside the
order processor's in-process path, so a test can assert on the decision without a
socket and the socket path can be tested without a model. Two implementations of
the *policy* would be a way to ship a policy that only holds in one of them.

Order of operations, and the order is load-bearing:

1. Pin the customer's rolling window, compute features.
2. Score with the model.
3. Classify into a band.
4. Decide whether an explanation is worth paying for, and produce one.
5. Record the order into the window -- **after** scoring, never before.
6. Emit metrics.

Step 5 after step 2 is the bug this ordering exists to prevent: recording first
would count an order toward its own velocity, inflating every score by one order
and making the feature look like constant noise.

Latency is measured here, around the whole thing, and is what the p95 budget is
checked against. The budget is a *product* requirement (200ms), not an
implementation detail, so the number that gets asserted lives next to the code
that produces it rather than in a Grafana panel nobody can fail a build on.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from backend.config.settings import Settings
from backend.core.clock import monotonic
from backend.core.logging import get_logger
from backend.events.schema import RiskBand
from backend.fraud.bands import band_for, rationale_worth_cost
from backend.fraud.features import FeatureVector, OrderContext, compute_features
from backend.fraud.llm import build_reasoner
from backend.fraud.model import Contribution, FraudModel, load_model_or_fallback
from backend.fraud.profile_window import CustomerWindow
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)


@dataclass(frozen=True)
class FraudDecision:
    """One scoring decision, in transport-independent form.

    `contributions` and `features` ride along because fraud ops asks "why" within
    minutes of a hold, and re-deriving the answer from the model a second time
    gives a *different* answer if the weights changed in between.
    """

    order_id: str
    customer_id: str
    score: float
    band: RiskBand
    reasons: list[str]
    rationale: str
    model_version: str
    latency_ms: float
    degraded: bool
    llm_used: bool
    contributions: list[Contribution] = field(default_factory=list)
    features: dict[str, float] = field(default_factory=dict)

    @property
    def auto_approvable(self) -> bool:
        """True when this decision may ship without a human looking at it.

        Deliberately a property of the decision rather than a comparison against
        a threshold at the call site: this is the policy, and the pipeline, the
        dashboard and the tests should all read it from one place.
        """
        return self.band == RiskBand.LOW and not self.degraded


class FraudScoringEngine:
    """Stateful: owns the rolling window that makes streaming features possible."""

    def __init__(
        self,
        settings: Settings,
        metrics: FlowMeshMetrics,
        *,
        model: FraudModel | None = None,
        degraded: bool = False,
    ) -> None:
        self._settings = settings
        self._metrics = metrics
        if model is None:
            model, degraded = load_model_or_fallback(settings.fraud_model_path)
        self.model = model
        self.degraded = degraded
        self.window = CustomerWindow(window_seconds=settings.fraud_velocity_window_seconds)
        self.reasoner = build_reasoner(
            llm_enabled=settings.llm_enabled,
            metrics=metrics,
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            timeout_seconds=settings.llm_timeout_seconds,
            max_tokens=settings.llm_max_tokens,
        )
        self.orders_scored = 0

    # -- the decision ------------------------------------------------------

    async def score(
        self,
        context: OrderContext,
        *,
        require_rationale: bool = False,
        transport: str = "in_process",
    ) -> FraudDecision:
        """Score one order and return the decision."""
        started = monotonic()

        # `pinned` rather than a bare `scoring_context`/`finally: release_context`
        # pair, because the pin is task-local state and leaking it is a cross-customer
        # data leak. See `CustomerWindow.pinned`. The `finally` that used to be here
        # did release it -- the problem was that `self._current` was a single slot
        # shared by every concurrent call, so a second score could overwrite the first
        # one's subject while the first was awaiting the reasoner.
        with self.window.pinned(context.customer_id):
            features = compute_features(context, self.window)
            score = self.model.score(features)
            band = band_for(
                score, self._settings.fraud_low_threshold, self._settings.fraud_high_threshold
            )
            contributions = self.model.contributions(features)
            reasons = self.model.reasons(features)

            rationale_text = ""
            llm_used = False
            if rationale_worth_cost(band, require_rationale):
                rationale = await self.reasoner.explain(
                    order_id=context.order_id,
                    score=score,
                    band=band,
                    contributions=contributions,
                    features=features,
                )
                rationale_text = rationale.text
                llm_used = rationale.llm_used
            elif reasons:
                # No LLM budget spent, but the reason codes still need prose for
                # the review screen. This is the deterministic path and it is
                # what the p95 budget is actually made of.
                rationale_text = _short_reason_text(score, band, contributions)

            decision = FraudDecision(
                order_id=context.order_id,
                customer_id=context.customer_id,
                score=score,
                band=band,
                reasons=reasons,
                rationale=rationale_text,
                model_version=self.model.version,
                latency_ms=(monotonic() - started) * 1000,
                degraded=self.degraded,
                llm_used=llm_used,
                contributions=contributions,
                features=features.as_dict(),
            )

        # Recorded after the decision, deliberately -- see the module docstring.
        self.window.record(
            customer_id=context.customer_id,
            total_cents=context.total_cents,
            card_bin=context.card_bin,
            at=context.received_at,
        )
        self.orders_scored += 1

        self._metrics.record_scoring(
            score=decision.score,
            band=str(decision.band),
            latency_seconds=decision.latency_ms / 1000.0,
            transport=transport,
        )
        if decision.degraded:
            self._metrics.scoring_degraded.labels(reason="model_unavailable").inc()

        logger.info(
            "scored order_id=%s customer_id=%s score=%.3f band=%s reasons=%s "
            "latency_ms=%.2f degraded=%s llm=%s",
            decision.order_id,
            decision.customer_id,
            decision.score,
            str(decision.band),
            ",".join(decision.reasons) or "-",
            decision.latency_ms,
            decision.degraded,
            decision.llm_used,
        )
        return decision

    # -- operations --------------------------------------------------------

    def mark_chargeback(self, customer_id: str) -> None:
        """Fold an out-of-band chargeback into the window."""
        self.window.mark_chargeback(customer_id)

    def tracked_customers(self) -> int:
        return len(self.window)

    def health(self) -> dict[str, object]:
        """Health payload, mirrored by the gRPC `Health` RPC."""
        return {
            "ready": True,
            "model_version": self.model.version,
            "orders_scored": self.orders_scored,
            "tracked_customers": self.tracked_customers(),
            "degraded": self.degraded,
        }


def _short_reason_text(score: float, band: RiskBand, contributions: list[Contribution]) -> str:
    """One sentence from the arithmetic, for the fast path.

    Two of the top contributors, named and valued. Anything longer belongs to the
    deterministic reasoner, which runs only where a human is actually going to
    read it.
    """
    drivers = [row for row in contributions if row.value > 0.0][:2]
    if not drivers:
        return f"Scored {score:.2f} ({band}) with no feature above its reporting floor."
    named = ", ".join(f"{row.label} ({row.value:.2f})" for row in drivers)
    return f"Scored {score:.2f} ({band}); driven by {named}."


def parse_received_at(raw: str) -> datetime:
    """Parse the request's timestamp, defaulting to now.

    Defaults rather than rejects: a missing timestamp costs the time-of-day
    feature, while rejecting costs the order. The feature is recoverable on the
    next order; the order is not.
    """
    from backend.core.clock import parse_isoformat_utc, utcnow

    if not raw:
        return utcnow()
    try:
        return parse_isoformat_utc(raw)
    except ValueError:
        logger.warning("unparseable received_at %r, using now", raw[:40])
        return utcnow()


def decision_from_features_for_test(model: FraudModel, features: FeatureVector) -> float:
    """Expose raw scoring, for the model unit tests."""
    return model.score(features)
