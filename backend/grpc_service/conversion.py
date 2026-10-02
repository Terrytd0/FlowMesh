"""Protobuf <-> domain conversion.

Kept out of both the server and the client so the two cannot disagree about what
a field means, and out of `backend/fraud/` so the engine never imports a
generated module. That last one is the important boundary: the scoring engine is
transport-free and unit-testable, and the moment `OrderContext` grew a protobuf
dependency, `pytest tests/unit/fraud` would need `grpcio` installed to test a
logistic regression.
"""

from __future__ import annotations

from datetime import datetime

from backend.fraud.engine import FraudDecision, parse_received_at
from backend.fraud.features import OrderContext
from backend.grpc_service.generated import fraud_pb2

_BAND_TO_PROTO = {
    "low": fraud_pb2.RISK_BAND_LOW,
    "ambiguous": fraud_pb2.RISK_BAND_AMBIGUOUS,
    "high": fraud_pb2.RISK_BAND_HIGH,
}


def context_from_request(request: fraud_pb2.FraudRequest) -> OrderContext:
    """Build the domain object from a wire request."""
    payment = request.payment
    return OrderContext(
        order_id=request.order_id,
        customer_id=request.customer_id,
        total_cents=int(request.total_cents),
        items=tuple((item.sku, int(item.quantity)) for item in request.items),
        card_bin=payment.card_bin,
        card_country=payment.card_country,
        billing_country=payment.billing_country,
        shipping_country=payment.shipping_country,
        ip_country=payment.ip_country,
        coupon_code=payment.coupon_code or None,
        is_gift_card=bool(payment.is_gift_card),
        received_at=parse_received_at(request.received_at),
    )


def response_from_decision(decision: FraudDecision) -> fraud_pb2.FraudResponse:
    """Render a decision onto the wire."""
    return fraud_pb2.FraudResponse(
        order_id=decision.order_id,
        score=decision.score,
        band=_BAND_TO_PROTO[str(decision.band)],
        reasons=list(decision.reasons),
        rationale=decision.rationale,
        model_version=decision.model_version,
        latency_ms=decision.latency_ms,
        degraded=decision.degraded,
        llm_used=decision.llm_used,
        contributions=[
            fraud_pb2.ScoreContribution(
                feature=contribution.feature,
                value=contribution.value,
                weight=contribution.weight,
                contribution=contribution.contribution,
            )
            for contribution in decision.contributions
        ],
        features={name: float(value) for name, value in decision.features.items()},
    )


def decision_from_response(response: fraud_pb2.FraudResponse) -> FraudDecision:
    """Rebuild a decision from a wire response, inside the client process.

    Round-tripping through protobuf rather than keeping the local object means
    the in-process transport and the gRPC transport are genuinely the same code
    path: if a field is missing from the schema, both transports break together
    instead of the gRPC one silently dropping it.
    """
    from backend.events.schema import RiskBand
    from backend.fraud.model import Contribution

    band = {
        fraud_pb2.RISK_BAND_LOW: RiskBand.LOW,
        fraud_pb2.RISK_BAND_AMBIGUOUS: RiskBand.AMBIGUOUS,
        fraud_pb2.RISK_BAND_HIGH: RiskBand.HIGH,
    }.get(response.band)
    if band is None:
        # UNSPECIFIED means the server is older than this client or the service
        # returned something unexpected. Failing closed -- treating it as the
        # most reviewable band -- is the only safe reading of "unknown".
        band = RiskBand.HIGH

    return FraudDecision(
        order_id=response.order_id,
        customer_id="",
        score=response.score,
        band=band,
        reasons=list(response.reasons),
        rationale=response.rationale,
        model_version=response.model_version,
        latency_ms=response.latency_ms,
        degraded=response.degraded,
        llm_used=response.llm_used,
        contributions=[
            Contribution(feature=row.feature, value=row.value, weight=row.weight)
            for row in response.contributions
        ],
        features=dict(response.features),
    )


def request_from_context(
    context: OrderContext, *, correlation_id: str, require_rationale: bool
) -> fraud_pb2.FraudRequest:
    """Build a wire request from a domain context (used by tests and the smoke script)."""
    return fraud_pb2.FraudRequest(
        order_id=context.order_id,
        customer_id=context.customer_id,
        items=[fraud_pb2.LineItem(sku=sku, quantity=quantity) for sku, quantity in context.items],
        payment=fraud_pb2.PaymentContext(
            card_bin=context.card_bin,
            card_country=context.card_country,
            billing_country=context.billing_country,
            shipping_country=context.shipping_country,
            ip_country=context.ip_country,
            coupon_code=context.coupon_code or "",
            is_gift_card=context.is_gift_card,
        ),
        total_cents=context.total_cents,
        received_at=context.received_at.isoformat(),
        correlation_id=correlation_id,
        require_rationale=require_rationale,
    )


def timestamp_of(raw: str) -> datetime:
    """Re-exported so callers do not import `parse_received_at` and guess."""
    return parse_received_at(raw)
