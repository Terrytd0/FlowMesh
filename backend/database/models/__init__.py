"""SQLAlchemy models: the system of record.

Read this file as the answer to "what is actually true", because in an
event-driven pipeline the log says what *happened* and this says what is
*currently believed*. When the two disagree, the reconciliation in
`scripts/chaos_test.py` reports the difference -- which is the only honest way to
find out.

Seven tables, grouped by what they answer:

| table | answers |
| --- | --- |
| `warehouses`, `inventory` | what stock exists, and where |
| `stock_reservations` | what is held for which order, and until when |
| `orders`, `order_items` | what was ordered |
| `fraud_scores` | why it was scored the way it was |
| `review_queue_items` | what a human still has to decide |
| `processed_events` | which events have already been applied |

`processed_events` is the one people skip and then cannot make idempotency work.
It is written in the *same transaction* as the effect it guards, which is the
only arrangement that actually prevents double-application: a separate "have I
seen this?" check before the write is a race, and a flag set after the write
leaves a window where the process died with the effect applied and the flag not
set.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.database.base import Base, TimestampMixin, UtcDateTime
from backend.events.schema import OrderStatus, ReservationState, ReviewStatus, RiskBand


class Warehouse(Base, TimestampMixin):
    """One of the twelve fulfilment centres."""

    __tablename__ = "warehouses"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    region: Mapped[str] = mapped_column(String(64), nullable=False)
    #: Whether this warehouse can ship internationally. Used by the shortfall
    #: handler: re-routing a domestic-only warehouse to an overseas customer
    #: produces an order that is technically reserved and practically undeliverable.
    ships_international: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    inventory: Mapped[list[Inventory]] = relationship(back_populates="warehouse")


class Inventory(Base, TimestampMixin):
    """Stock of one SKU at one warehouse.

    `available` is what can still be promised. `on_hand` is what physically
    exists. The gap between them is `reserved`, and it is stored as a derived
    column rather than computed, because `on_hand - reserved` computed on read is
    a subtraction under concurrency and a rounding error waiting to become an
    oversell.
    """

    __tablename__ = "inventory"

    warehouse_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("warehouses.id", ondelete="CASCADE"), primary_key=True
    )
    sku: Mapped[str] = mapped_column(String(64), primary_key=True)
    on_hand: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reserved: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    #: Denormalised available stock. Kept in step by the conditional UPDATE in
    #: `backend/inventory/service.py`, which is the only writer.
    available: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    #: Bumped on every mutation. Lets the reconciliation check compare "expected"
    #: against "actual" without replaying the whole log.
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        CheckConstraint("on_hand >= 0", name="on_hand_non_negative"),
        CheckConstraint("reserved >= 0", name="reserved_non_negative"),
        CheckConstraint("available >= 0", name="available_non_negative"),
        # The invariant the whole inventory service exists to keep, enforced by
        # the database as well as by the code. Defence in depth is not a slogan
        # here: this constraint is what makes an oversell impossible even if a
        # future bug bypasses the conditional UPDATE.
        CheckConstraint("available = on_hand - reserved", name="available_consistent"),
        Index("ix_inventory_sku", "sku"),
    )

    warehouse: Mapped[Warehouse] = relationship(back_populates="inventory")


class Order(Base, TimestampMixin):
    """A customer order, as currently believed.

    `status` is the only thing the API reads to answer "where is my order", and
    every transition writes an `audit_log` row. The status is therefore a cache
    of the audit trail, not the record of it -- which is why the reconciliation
    can rebuild it from events and compare.
    """

    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[OrderStatus] = mapped_column(
        String(24), nullable=False, default=OrderStatus.ACCEPTED
    )
    total_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    #: The caller's Idempotency-Key. Unique, so a retried POST cannot create two
    #: orders even if the two requests land on different replicas.
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    #: Set when the order is held; the reason is a human-readable summary.
    hold_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: Denormalised score, so the review dashboard can sort by risk without
    #: joining. Nullable: an order exists for a moment before it is scored.
    fraud_score: Mapped[float | None] = mapped_column(nullable=True)
    fraud_band: Mapped[RiskBand | None] = mapped_column(String(16), nullable=True)

    __table_args__ = (
        CheckConstraint("total_cents >= 0", name="total_non_negative"),
        CheckConstraint(
            "fraud_score IS NULL OR (fraud_score >= 0.0 AND fraud_score <= 1.0)",
            name="fraud_score_range",
        ),
        Index("ix_orders_status_created", "status", "created_at"),
    )

    items: Mapped[list[OrderItem]] = relationship(
        back_populates="order", cascade="all, delete-orphan"
    )


class OrderItem(Base):
    """One line of an order."""

    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(
        String(40), ForeignKey("orders.id", ondelete="CASCADE"), index=True
    )
    sku: Mapped[str] = mapped_column(String(64), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint("unit_price_cents >= 0", name="unit_price_non_negative"),
        UniqueConstraint("order_id", "sku", name="uq_order_items_order_sku"),
    )

    order: Mapped[Order] = relationship(back_populates="items")


class StockReservation(Base, TimestampMixin):
    """Stock held for one order at one warehouse.

    The state machine is `held -> committed | released | expired`, and it is
    enforced by a check constraint rather than by application code alone: the
    expiry sweeper runs in a separate process from the reservation service, and
    two processes agreeing on a rule is how "released" ends up meaning two
    different things in two log lines.
    """

    __tablename__ = "stock_reservations"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    warehouse_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    state: Mapped[ReservationState] = mapped_column(
        String(16), nullable=False, default=ReservationState.HELD
    )
    #: When the hold lapses. Indexed because the sweeper's only query is
    #: "held rows past this time", and an unindexed timestamp scan on a table
    #: that grows with every order is a table scan per sweep.
    expires_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, index=True)
    released_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(128), nullable=True)

    #: The (sku, quantity) pairs this hold covers, snapshotted at hold time.
    #:
    #: Deliberately duplicated from `order_items`. The release path returns units
    #: using this column, so a hold must be able to state what it holds without
    #: consulting a table that can change underneath it. Reading the lines by
    #: joining `order_items` instead makes the release a silent no-op whenever
    #: those rows are absent -- and the order rows are written by a *different*
    #: transaction from the one that creates this reservation, so "absent" is a
    #: reachable state, not a hypothetical one.
    lines: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)

    __table_args__ = (
        UniqueConstraint("order_id", "warehouse_id", name="uq_reservation_order_warehouse"),
        Index("ix_reservations_expiry", "state", "expires_at"),
    )


class FraudScore(Base):
    """One scoring decision, kept with its explanation.

    Append-only by convention. A re-scored order gets a second row with a later
    `decided_at`, not an update -- because the question "what did the system
    decide at 14:03, and was that right?" is unanswerable if the row has been
    overwritten, and it is the first question asked after any fraud incident.
    """

    __tablename__ = "fraud_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    event_id: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    score: Mapped[float] = mapped_column(nullable=False)
    band: Mapped[RiskBand] = mapped_column(String(16), nullable=False)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    reasons: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    rationale: Mapped[str] = mapped_column(Text, nullable=False, default="")
    #: Full per-feature breakdown. Stored rather than recomputed on demand
    #: because the weights file can change, and a re-derivation would answer a
    #: question about *today's* model rather than about the decision being
    #: reviewed.
    contributions: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    degraded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    llm_used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    transport: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    latency_ms: Mapped[float] = mapped_column(nullable=False, default=0.0)
    decided_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, server_default=None)

    __table_args__ = (
        CheckConstraint("score >= 0.0 AND score <= 1.0", name="score_range"),
        Index("ix_fraud_scores_band_decided", "band", "decided_at"),
    )


class ReviewQueueItem(Base, TimestampMixin):
    """An order waiting for a human, or the record that one already decided it."""

    __tablename__ = "review_queue_items"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)
    status: Mapped[ReviewStatus] = mapped_column(
        String(16), nullable=False, default=ReviewStatus.PENDING
    )
    score: Mapped[float] = mapped_column(nullable=False)
    band: Mapped[RiskBand] = mapped_column(String(16), nullable=False)
    reasons: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    rationale: Mapped[str] = mapped_column(Text, nullable=False, default="")
    #: When the SLA clock started. `None` once decided; a decided item with a live
    #: clock shows up on the dashboard as "breached" forever.
    sla_deadline: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, index=True)
    decided_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    decision_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: The message id from the task queue, so a redelivered review request can be
    #: recognised as the same request rather than a second one.
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)

    __table_args__ = (
        Index("ix_review_status_sla", "status", "sla_deadline"),
        # A decided item must say who decided and when. Without this, an approval
        # with no actor is a row that a later audit cannot attribute to anyone.
        CheckConstraint(
            "(status = 'pending') OR (decided_by IS NOT NULL AND decided_at IS NOT NULL)",
            name="decided_has_actor",
        ),
    )


class ProcessedEvent(Base):
    """Idempotency ledger: one row per applied event.

    `event_id` is the primary key, so the insert *is* the duplicate check -- a
    concurrent second delivery collides on the constraint and loses, with no
    window between "have I seen it" and "I am writing it".

    `effect` records what the handler did, which turns this table from a set of
    ids into the reconciliation index: given the log and this table, "was every
    published event applied" is a join, not an argument.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    topic: Mapped[str] = mapped_column(String(64), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(64), nullable=False)
    order_id: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    #: Free-form description of the applied effect, e.g. `reserved:2`.
    effect: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    #: Partition/offset the event came from. Kept so a log offset can be traced
    #: to the row it produced without searching the log again.
    partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    log_offset: Mapped[int | None] = mapped_column(Integer, nullable=True)
    applied_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)

    __table_args__ = (
        # A composite index on (group, applied_at) serves the two operational
        # queries: "what did this consumer do recently" and "has this group
        # stalled".
        Index("ix_processed_group_applied", "consumer_group", "applied_at"),
    )


class Notification(Base):
    """An outbound customer or reviewer notification.

    An outbox: written in the same transaction as the decision that caused it,
    and delivered by a separate worker. That is why `sent_at` may be null on a
    row that exists -- and why the review dashboard can count "approved but the
    customer has not been told yet", which is the failure mode where the pipeline
    is healthy and the customer experience is not.
    """

    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    channel: Mapped[str] = mapped_column(String(16), nullable=False)
    template: Mapped[str] = mapped_column(String(64), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    sent_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)

    __table_args__ = (Index("ix_notifications_unsent", "sent_at", "created_at"),)


class AuditLog(Base):
    """Append-only record of every state transition and every agent decision.

    Nothing in this project updates or deletes a row here. `actor` is a string
    rather than a foreign key because the actors are not only users: they are
    `pipeline:order-processor`, `worker:review`, and `system:expiry-sweeper`, and
    a FK to a users table would force those to be fabricated as users.
    """

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    entity: Mapped[str] = mapped_column(String(48), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(48), nullable=False)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    #: Structured detail. JSON rather than a message column so "every transition
    #: to rejected" is a query, not a `LIKE`.
    detail: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    correlation_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)

    __table_args__ = (
        Index("ix_audit_entity", "entity", "entity_id"),
        Index("ix_audit_created", "created_at"),
    )
