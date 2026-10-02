"""Request and response schemas for the API.

`OrderCreateRequest` is the one that matters. Three constraints on it, each
there because of a specific failure:

- **`items` has at least one entry and a hard cap.** An empty basket is a
  validation error at the edge rather than an order that fails three services
  later; the cap is 200 lines because an unbounded list is a denial-of-service
  vector on a public endpoint.
- **`quantity` is capped at 9999.** A single line of 2^31 units is a request that
  will be accepted, published, scored and then rejected by every warehouse --
  four services' worth of work for a guaranteed failure.
- **No card number.** `PaymentIn` takes a BIN and last four. Accepting a full PAN
  here would put it in the event log, the review dashboard and the
  `fraud_scores` row, which is a PCI scope this project does not want.

The response deliberately does not include `hold_reason` for a low-score order:
a caller should not be able to learn the model internals by probing. A held order
gets a generic status, and the reason is available only to a supervisor.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from backend.events.schema import OrderStatus, RiskBand

Cents = Annotated[int, Field(ge=0, le=100_000_000)]
Quantity = Annotated[int, Field(ge=1, le=9999)]


class LineItemIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: Annotated[str, Field(min_length=1, max_length=64)]
    quantity: Quantity
    unit_price_cents: Cents = 0


class PaymentIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bin: Annotated[str, Field(min_length=6, max_length=6)]
    last4: Annotated[str, Field(min_length=4, max_length=4)]
    card_country: Annotated[str, Field(min_length=2, max_length=2)]
    billing_country: Annotated[str, Field(min_length=2, max_length=2)]
    shipping_country: Annotated[str, Field(min_length=2, max_length=2)]
    ip_country: Annotated[str, Field(min_length=2, max_length=2)] = "ZZ"
    coupon_code: Annotated[str | None, Field(max_length=64)] = None
    is_gift_card: bool = False


class OrderCreateRequest(BaseModel):
    """`POST /orders` body."""

    model_config = ConfigDict(extra="forbid")

    customer_id: Annotated[str, Field(min_length=1, max_length=64)]
    items: Annotated[list[LineItemIn], Field(min_length=1, max_length=200)]
    payment: PaymentIn
    #: Optional; the server computes it from the items when absent. Accepted
    #: explicitly because a client with a stale cart has been known to send one,
    #: and silently recomputing hides the disagreement rather than resolving it.
    total_cents: Cents | None = None

    def computed_total(self) -> int:
        return sum(item.unit_price_cents * item.quantity for item in self.items)

    def merged_items(self) -> list[LineItemIn]:
        """Line items with duplicate SKUs combined, prices checked for agreement.

        A client sending `[{SKU-A, 1}, {SKU-A, 2}]` means three of SKU-A, and
        `order_items` enforces `UNIQUE (order_id, sku)`. Merging here -- at the
        boundary, where the untrusted request is -- is the only place it can be
        done without a database round trip on the hot path.

        **Mismatched prices for the same SKU are rejected, not merged.** If a
        client says SKU-A costs 100 on one line and 5000 on another, the request
        is inconsistent, and quietly picking the lower price would be a
        manipulation vector. The API is not the place to guess which one was
        meant.
        """
        merged: dict[str, LineItemIn] = {}
        for item in self.items:
            existing = merged.get(item.sku)
            if existing is None:
                merged[item.sku] = item.model_copy()
                continue
            if existing.unit_price_cents != item.unit_price_cents:
                raise ValueError(
                    f"item {item.sku!r} appears twice with different unit prices "
                    f"({existing.unit_price_cents} and {item.unit_price_cents})"
                )
            existing.quantity += item.quantity
        return list(merged.values())


class OrderAcceptedResponse(BaseModel):
    """The 202 from `POST /orders`."""

    order_id: str
    status: OrderStatus
    #: Echoed so a client can correlate its own logs without a second call.
    correlation_id: str
    #: True when this was a replay of an idempotency key and no new order was made.
    idempotent_replay: bool = False
    #: Present only for a held order, and only a generic reason -- never the
    #: model internals.
    held_reason: str | None = None


class LineItemOut(BaseModel):
    sku: str
    quantity: int
    unit_price_cents: int


class FraudScoreOut(BaseModel):
    score: float
    band: RiskBand
    model_version: str
    reasons: list[str]
    rationale: str
    degraded: bool
    llm_used: bool
    latency_ms: float
    decided_at: datetime


class OrderOut(BaseModel):
    order_id: str
    customer_id: str
    status: OrderStatus
    total_cents: int
    items: list[LineItemOut]
    fraud_score: FraudScoreOut | None = None
    hold_reason: str | None = None
    created_at: datetime
    updated_at: datetime


class ReviewItemOut(BaseModel):
    review_id: str
    order_id: str
    status: str
    score: float
    band: RiskBand
    reasons: list[str]
    rationale: str
    sla_deadline: datetime
    decided_by: str | None
    decided_at: datetime | None
    decision_note: str | None
    #: Seconds until the SLA deadline; negative means already breached. Computed
    #: server-side so the dashboard and the alerting rule cannot disagree about
    #: what "late" means.
    seconds_to_breach: float


class ReviewDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: str = Field(pattern="^(approved|rejected)$")
    note: Annotated[str, Field(max_length=500)] = ""


class ReviewStatsOut(BaseModel):
    pending: int
    sla_breached: int
    oldest_pending_age_seconds: float
    by_status: dict[str, int]


class InventoryRowOut(BaseModel):
    warehouse_id: str
    sku: str
    on_hand: int
    reserved: int
    available: int
    version: int


class WarehouseOut(BaseModel):
    id: str
    name: str
    region: str
    ships_international: bool


class TimelineEntry(BaseModel):
    created_at: datetime
    entity: str
    entity_id: str
    action: str
    actor: str
    detail: dict[str, Any]


class HealthOut(BaseModel):
    status: str
    database: str
    event_transport: str
    queue_transport: str
    fraud_transport: str
    model_version: str
    orders: int
    degraded: bool


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: Annotated[str, Field(min_length=3, max_length=256)]
    password: Annotated[str, Field(min_length=1, max_length=256)]


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    role: str
    subject: str
