"""The event contract.

One envelope, one payload-per-event-type, and **no free-form dicts on the
wire**. Everything that crosses a process boundary is a Pydantic model that
round-trips through JSON here, so a schema change that breaks a consumer is a
validation error at the boundary rather than a `KeyError` three processes later
at 3am.

Two families of event, and they mean different things:

- **Facts** (`order.accepted`, `order.scored`, `inventory.reserved`) are
  immutable statements about something that happened. They are named in the
  past tense because a fact cannot be edited: if it changes, a new fact
  supersedes it, and the log keeps both.
- **Commands** (`review.requested`) are requests for something to happen. They
  may be retried, and are at-least-once by definition.

The distinction is not academic: a fact replayed produces the same state, a
command replayed produces a second review. Every handler in this project keys
its idempotency on `event_id` and the two are treated differently downstream --
facts key on `(event_id)`, commands key on their business identity
(`order_id`) so a redelivered command is a no-op rather than a duplicate.

Envelopes carry the fields an operator needs to reconstruct an order without a
database: `event_id`, `event_type`, `occurred_at`, `correlation_id`, `order_id`,
`schema_version`, plus a monotonic `offset` filled in by the broker (or by the
in-process log) rather than by the producer.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.core.clock import isoformat_utc, utcnow
from backend.core.ids import new_correlation_id, new_event_id

SCHEMA_VERSION = 1


class EventType(StrEnum):
    """Every event type on the log.

    A `StrEnum` rather than bare strings so a typo is an `AttributeError` at
    import time. The values are what appears on the wire, so they are frozen
    API: renaming one is a breaking change to every consumer, including the
    ones deployed independently.
    """

    ORDER_ACCEPTED = "order.accepted"
    ORDER_SCORED = "order.scored"
    ORDER_HELD = "order.held"
    ORDER_APPROVED = "order.approved"
    ORDER_REJECTED = "order.rejected"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_RELEASED = "inventory.released"
    INVENTORY_COMMITTED = "inventory.committed"
    INVENTORY_SHORTFALL = "inventory.shortfall"


class Topic(StrEnum):
    """Kafka topics.

    Two topics, not one, because they have different retention, different
    partitioning keys and different consumers. `order-events` is keyed by
    `order_id` -- every event about one order must land on one partition so a
    consumer sees them in order. `inventory-events` is keyed by SKU so that all
    movements of one SKU serialise against each other, which is what makes the
    stock ledger reconstructible from the log.
    """

    ORDER = "order-events"
    INVENTORY = "inventory-events"


class RiskBand(StrEnum):
    """Which side of the thresholds a score fell on."""

    LOW = "low"
    AMBIGUOUS = "ambiguous"
    HIGH = "high"


class OrderStatus(StrEnum):
    """Order lifecycle.

    `REVIEW` is a real state, not an absence of one. Modelling "being held" as
    a status is what lets the SLA clock, the review queue and the customer's
    "where is my order" page all read the same column.
    """

    ACCEPTED = "accepted"
    SCORED = "scored"
    RESERVED = "reserved"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


class ReviewStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class ReservationState(StrEnum):
    HELD = "held"
    COMMITTED = "committed"
    RELEASED = "released"
    EXPIRED = "expired"


# --------------------------------------------------------------------------
# Envelope
# --------------------------------------------------------------------------


class EventEnvelope(BaseModel):
    """The wrapper every event travels in."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(default_factory=new_event_id)
    event_type: EventType
    occurred_at: datetime = Field(default_factory=utcnow)
    correlation_id: str = Field(default_factory=new_correlation_id)
    order_id: str | None = None
    #: Filled in by the log, not the producer. `None` for a published-but-not-yet-
    #: stored envelope, which is why it is optional and why nothing in a
    #: producer is allowed to set it.
    offset: int | None = None
    partition: int | None = None
    schema_version: int = SCHEMA_VERSION
    payload: dict[str, Any] = Field(default_factory=dict)

    @property
    def occurred_at_iso(self) -> str:
        return isoformat_utc(self.occurred_at)

    def to_wire(self) -> bytes:
        """Serialise to the bytes the broker stores.

        `mode="json"` renders `occurred_at` as an ISO-8601 string rather than a
        datetime object, so the bytes are stable across pydantic versions. A
        consumer on a different version must be able to parse what a producer
        wrote, and the timestamp is the one field both sides always agree on.

        Returns `bytes`, not a dict: the broker's value is bytes, and a producer
        that "helpfully" handed it a dict would be serialising the envelope a
        second time with whatever default the client's JSON encoder happened to
        use.
        """
        body = self.model_dump(mode="json")
        body["event_type"] = str(self.event_type)
        return json.dumps(body, separators=(",", ":"), default=str).encode("utf-8")

    def to_dict(self) -> dict[str, Any]:
        """The same payload as a dict, for the in-process log and for tests."""
        body = self.model_dump(mode="json")
        body["event_type"] = str(self.event_type)
        return body

    @classmethod
    def from_wire(cls, raw: bytes | str) -> EventEnvelope:
        """Parse from the broker, validating."""
        import json

        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        return cls.model_validate(json.loads(text))


# --------------------------------------------------------------------------
# Payloads
# --------------------------------------------------------------------------


class LineItem(BaseModel):
    """One line of an order.

    `sku` and `quantity` only. Price is deliberately absent: the fraud scorer
    must not be able to see a price and reason about margin, because a fraud
    model that learns "high value means fraud" is a fraud model that rejects
    every legitimate customer's Christmas order.
    """

    sku: str
    quantity: int = Field(ge=1)
    unit_price_cents: int = Field(ge=0)


class PaymentDetails(BaseModel):
    """Payment context for scoring.

    A masked PAN and a BIN, never a full card number. Storing a full PAN here
    would make this log a PCI-DSS scope, which is a compliance project of its
    own and not what this sprint is about.

    Three countries, because one is not enough to spot the classic pattern: a
    card issued in one country, billed in a second, shipped to a third, from an
    IP in a fourth. Each is a weak signal alone and a strong one together, and
    the scorer weights them separately for exactly that reason.
    """

    bin: str
    last4: str
    card_country: str
    billing_country: str
    shipping_country: str
    ip_country: str
    coupon_code: str | None = None
    is_gift_card: bool = False


class OrderAcceptedPayload(BaseModel):
    """A newly accepted order, published by the ingress API."""

    order_id: str
    customer_id: str
    items: list[LineItem] = Field(min_length=1)
    payment: PaymentDetails
    total_cents: int = Field(ge=0)
    idempotency_key: str
    received_at: datetime = Field(default_factory=utcnow)


class OrderScoredPayload(BaseModel):
    """A scoring decision, published after the gRPC call returns."""

    order_id: str
    customer_id: str
    score: float = Field(ge=0.0, le=1.0)
    band: RiskBand
    reasons: list[str] = Field(default_factory=list)
    rationale: str = ""
    model_version: str = "unknown"
    latency_ms: float = 0.0
    transport: str = "unknown"
    degraded: bool = False


class OrderDecisionPayload(BaseModel):
    """An approval or rejection after human review."""

    order_id: str
    review_id: str
    decided_by: str
    decision: Literal["approved", "rejected"]
    note: str = ""


class InventoryLine(BaseModel):
    sku: str
    quantity: int = Field(ge=1)


class InventoryReservedPayload(BaseModel):
    """Stock held for an order, at one warehouse."""

    order_id: str
    warehouse_id: str
    lines: list[InventoryLine] = Field(min_length=1)
    reservation_id: str
    expires_at: datetime


class InventoryReleasedPayload(BaseModel):
    order_id: str
    warehouse_id: str
    reservation_id: str
    reason: str


class InventoryCommittedPayload(BaseModel):
    order_id: str
    warehouse_id: str
    reservation_id: str


class InventoryShortfallPayload(BaseModel):
    """A reservation that could not be satisfied.

    Published rather than logged because it is the one thing in this pipeline
    that means a customer is told their item is unavailable, and the recovery
    path (re-route to another warehouse, backorder, refund) is a consumer
    decision, not the reservation service's.
    """

    order_id: str
    warehouse_id: str
    requested: list[InventoryLine] = Field(min_length=1)
    reason: str


def envelope_for(event_type: EventType, payload: BaseModel, **context: Any) -> EventEnvelope:
    """Build an envelope, taking correlation/order ids from the payload.

    Every producer goes through here, so `correlation_id` and `order_id` cannot
    drift between the order processor and the inventory worker -- which is
    exactly the drift that makes a pipeline un-debuggable at 2am.
    """
    order_id = context.pop("order_id", None) or getattr(payload, "order_id", None)
    correlation_id = context.pop("correlation_id", None) or getattr(payload, "correlation_id", None)
    return EventEnvelope(
        event_type=event_type,
        order_id=order_id,
        correlation_id=correlation_id or new_correlation_id(),
        payload=payload.model_dump(mode="json"),
        **context,
    )
