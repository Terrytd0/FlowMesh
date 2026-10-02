"""Inventory persistence, including the conditional decrement.

**This module is the oversell guarantee.** Everything the sprint claims about
"stock never goes negative" bottoms out in `reserve_units`.

The naive version reads then writes:

```python
row = await session.get(Inventory, (warehouse, sku))
if row.available >= qty:
    row.available -= qty          # someone else got here first
```

Between the read and the write there is no lock, so two concurrent reservations
both see the same `available` and both subtract. At 10 units and 500 concurrent
orders, the naive version sells 500. Not occasionally -- reliably, because the
race window is wide at that rate.

The version here issues one statement and trusts `rowcount`:

```sql
UPDATE inventory
   SET available = available - :qty,
       reserved  = reserved + :qty,
       version   = version + 1
 WHERE warehouse_id = :wh AND sku = :sku AND available >= :qty
```

The database holds the row's write lock for the duration, so the comparison and
the decrement are atomic against every other writer. `rowcount == 0` means either
the row is missing or there is not enough stock, and both are refusals.

The check constraint `available = on_hand - reserved` backs this up. A bug
elsewhere that writes `available` directly without touching `reserved` fails at
the database, not in a customer's hands.

There is a second, subtler hazard here: on PostgreSQL the row lock is held to the
*end of the transaction*, not the end of the statement. Two transactions
reserving for the same SKU serialise on that lock, which is correct, and it is
also why the reserve path must not do anything slow (no LLM call, no HTTP) inside
its transaction. `backend/inventory/service.py` keeps the transaction to the
smallest possible scope for exactly this reason.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.clock import as_utc, utcnow
from backend.core.ids import new_event_id
from backend.database.models import Inventory, StockReservation, Warehouse
from backend.events.schema import ReservationState


def rows_affected(result: Any) -> int:
    """`rowcount` from a DML result, normalised to an int.

    SQLAlchemy types `AsyncSession.execute` as returning `Result`, but for an
    `UPDATE` the row count lives on the `CursorResult` subtype and mypy cannot see
    it from here. A typing seam, not a runtime cast -- the value has always been
    there, and it is the value the whole oversell guard depends on, so it gets one
    named accessor instead of six scattered `getattr` calls.
    """
    return int(getattr(result, "rowcount", 0) or 0)


async def seed_warehouses(session: AsyncSession, rows: list[dict[str, Any]]) -> int:
    """Insert warehouses, skipping any that already exist.

    Idempotent because `make seed` is run against a database that may already
    have data, and a seed that fails halfway is a seed that leaves the project in
    a state nobody wants to debug.
    """
    existing = set(
        (await session.execute(select(Warehouse.id))).scalars()  # type: ignore[arg-type]
    )
    inserted = 0
    for row in rows:
        if row["id"] in existing:
            continue
        session.add(
            Warehouse(
                id=row["id"],
                name=row["name"],
                region=row["region"],
                ships_international=bool(row.get("ships_international", True)),
            )
        )
        inserted += 1
    await session.flush()
    return inserted


async def seed_inventory(
    session: AsyncSession, rows: list[dict[str, Any]], *, overwrite: bool = False
) -> int:
    """Insert or update stock levels. `overwrite=False` leaves existing rows alone."""
    written = 0
    for row in rows:
        warehouse_id = row["warehouse_id"]
        sku = row["sku"]
        on_hand = int(row["on_hand"])
        if overwrite:
            statement = (
                update(Inventory)
                .where(Inventory.warehouse_id == warehouse_id, Inventory.sku == sku)
                .values(
                    on_hand=on_hand, reserved=0, available=on_hand, version=Inventory.version + 1
                )
            )
            result = await session.execute(statement)
            if rows_affected(result):
                written += rows_affected(result)
                continue
        session.add(
            Inventory(
                warehouse_id=warehouse_id,
                sku=sku,
                on_hand=on_hand,
                reserved=0,
                available=on_hand,
                version=0,
            )
        )
        written += 1
    await session.flush()
    return written


async def get_inventory(session: AsyncSession, warehouse_id: str, sku: str) -> Inventory | None:
    """Read one row, refreshing it from the database.

    `populate_existing` is load-bearing, not a micro-optimisation. Every mutation
    in this module is a Core `UPDATE`, which the ORM does not know about: without
    it, a row already in the session's identity map keeps its *pre-update* values
    and the caller reads stale stock. That produces a test that fails against a
    correct implementation, and -- far worse -- a warehouse-picking routine that
    reads `available` from a cached object and sends the order to a warehouse that
    was emptied a moment ago.
    """
    result = await session.execute(
        select(Inventory)
        .where(Inventory.warehouse_id == warehouse_id, Inventory.sku == sku)
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def list_inventory(session: AsyncSession, *, sku: str | None = None) -> list[Inventory]:
    """List rows, refreshing any already in the identity map. See `get_inventory`."""
    statement = (
        select(Inventory)
        .order_by(Inventory.warehouse_id, Inventory.sku)
        .execution_options(populate_existing=True)
    )
    if sku is not None:
        statement = statement.where(Inventory.sku == sku)
    result = await session.execute(statement)
    return list(result.scalars())


async def total_available(session: AsyncSession, sku: str) -> int:
    """Total available across every warehouse, for the shortfall handler."""
    result = await session.execute(
        select(func.coalesce(func.sum(Inventory.available), 0)).where(Inventory.sku == sku)
    )
    return int(result.scalar_one())


async def reserve_units(
    session: AsyncSession, *, warehouse_id: str, sku: str, quantity: int
) -> bool:
    """Take `quantity` units off `available`. `False` means there were not enough.

    The conditional decrement. See the module docstring for why this is one
    statement and not a read followed by a write.
    """
    if quantity < 1:
        raise ValueError(f"quantity must be >= 1, got {quantity}")
    statement = (
        update(Inventory)
        .where(
            Inventory.warehouse_id == warehouse_id,
            Inventory.sku == sku,
            Inventory.available >= quantity,
        )
        .values(
            available=Inventory.available - quantity,
            reserved=Inventory.reserved + quantity,
            version=Inventory.version + 1,
            updated_at=utcnow(),
        )
    )
    result = await session.execute(statement)
    return bool(rows_affected(result))


async def release_units(
    session: AsyncSession, *, warehouse_id: str, sku: str, quantity: int
) -> bool:
    """Return `quantity` held units to `available`.

    Guarded on `reserved >= quantity` for the same reason the reserve is guarded
    on `available >= quantity`. An unguarded release is how stock appears from
    nowhere after a double-release -- which looks exactly like an oversell on the
    balance sheet, and takes the same investigation to find.
    """
    if quantity < 1:
        raise ValueError(f"quantity must be >= 1, got {quantity}")
    statement = (
        update(Inventory)
        .where(
            Inventory.warehouse_id == warehouse_id,
            Inventory.sku == sku,
            Inventory.reserved >= quantity,
        )
        .values(
            available=Inventory.available + quantity,
            reserved=Inventory.reserved - quantity,
            version=Inventory.version + 1,
            updated_at=utcnow(),
        )
    )
    result = await session.execute(statement)
    return bool(rows_affected(result))


async def commit_units(
    session: AsyncSession, *, warehouse_id: str, sku: str, quantity: int
) -> bool:
    """Convert held units into shipped units: `reserved` down, `on_hand` down.

    `available` does not move, because the units were already unavailable while
    held. Getting that wrong -- decrementing `available` as well -- is a
    double-count that quietly inflates apparent stock.
    """
    if quantity < 1:
        raise ValueError(f"quantity must be >= 1, got {quantity}")
    statement = (
        update(Inventory)
        .where(
            Inventory.warehouse_id == warehouse_id,
            Inventory.sku == sku,
            Inventory.reserved >= quantity,
        )
        .values(
            reserved=Inventory.reserved - quantity,
            on_hand=Inventory.on_hand - quantity,
            version=Inventory.version + 1,
            updated_at=utcnow(),
        )
    )
    result = await session.execute(statement)
    return bool(rows_affected(result))


async def create_reservation(
    session: AsyncSession,
    *,
    order_id: str,
    warehouse_id: str,
    ttl_seconds: int,
    reservation_id: str | None = None,
    lines: list[dict[str, Any]] | None = None,
) -> StockReservation:
    """Record the hold, snapshotting the lines it covers.

    `lines` is required for the release path to work; it defaults to empty only so
    the existing callers keep compiling, and an empty snapshot is precisely the
    case `reservation_lines` documents as the silent-failure trap. Every
    production caller passes it.

    The unique constraint on `(order_id, warehouse_id)` is what makes a
    redelivered `inventory.reserved` event idempotent at the row level: the
    second insert fails, and the handler treats that as "already held".
    """
    reservation = StockReservation(
        id=reservation_id or new_event_id(),
        order_id=order_id,
        warehouse_id=warehouse_id,
        state=ReservationState.HELD,
        expires_at=utcnow() + timedelta(seconds=ttl_seconds),
        lines=[
            {"sku": str(line["sku"]), "quantity": int(line["quantity"])} for line in (lines or [])
        ],
    )
    session.add(reservation)
    await session.flush()
    return reservation


async def get_reservation(
    session: AsyncSession, *, order_id: str, warehouse_id: str
) -> StockReservation | None:
    """One reservation, refreshed from the database.

    `populate_existing` because the `state` column is changed by Core `UPDATE`s in
    the same session that reads it. Without the refresh a caller sees `held` after
    the units have already been released -- and then acts on a reservation that no
    longer exists, which is how a double-release happens.
    """
    result = await session.execute(
        select(StockReservation)
        .where(
            StockReservation.order_id == order_id,
            StockReservation.warehouse_id == warehouse_id,
        )
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def set_reservation_state(
    session: AsyncSession,
    *,
    reservation_id: str,
    state: ReservationState,
    reason: str | None = None,
    released_at: datetime | None = None,
) -> bool:
    """Move a reservation to a new state.

    Returns `False` when the reservation was not found, which the caller reads as
    "nothing to do" -- the normal outcome of a redelivered release.
    """
    values: dict[str, Any] = {"state": state, "updated_at": utcnow()}
    if reason is not None:
        values["reason"] = reason
    if released_at is not None:
        values["released_at"] = released_at
    elif state in (ReservationState.RELEASED, ReservationState.COMMITTED, ReservationState.EXPIRED):
        values["released_at"] = utcnow()
    result = await session.execute(
        update(StockReservation).where(StockReservation.id == reservation_id).values(**values)
    )
    return bool(rows_affected(result))


async def expired_reservations(
    session: AsyncSession, *, limit: int = 200, now: datetime | None = None
) -> list[StockReservation]:
    """Held reservations past their TTL, oldest first.

    The expiry sweeper's only query. The `(state, expires_at)` index exists
    entirely for it: without it, every sweep is a full scan of a table that grows
    by one row per order, run every thirty seconds, forever.

    The comparison parameter goes through `as_utc` because SQLite hands back
    naive datetimes for a `DateTime(timezone=True)` column -- see
    `backend/core/clock.py::as_utc`. Comparing a naive `expires_at` against an
    aware `utcnow()` raises, and it would raise *only* on the SQLite path, which
    is the path the tests and the load test use.
    """
    moment = as_utc(now) or utcnow()
    result = await session.execute(
        select(StockReservation)
        .where(
            StockReservation.state == ReservationState.HELD, StockReservation.expires_at < moment
        )
        .order_by(StockReservation.expires_at)
        .limit(limit)
        .execution_options(populate_existing=True)
    )
    return list(result.scalars())


async def active_reservation_count(session: AsyncSession) -> int:
    result = await session.execute(
        select(func.count())
        .select_from(StockReservation)
        .where(StockReservation.state == ReservationState.HELD)
    )
    return int(result.scalar_one())


def reservation_lines(reservation: StockReservation) -> list[tuple[str, int]]:
    """The (sku, quantity) pairs a reservation holds, from the row itself.

    Read from `stock_reservations.lines`, **not** joined from `order_items`.

    The join version was the first implementation and it is a trap. The release
    path is what returns the units, and reading the lines from another table
    means the units come back only if that table still says what it said when the
    hold was created. If the order rows are missing, deleted, or written *after*
    the reservation -- all of which are possible, and the third is the normal
    ordering when a producer reserves before persisting -- the release becomes a
    silent no-op: `expire_reservation` reports success, the state moves to
    `expired`, and the stock stays held forever with nothing recording that.

    Found by `test_the_sweeper_reclaims_an_expired_hold`, which reserves stock
    without creating order rows and watches the expiry do nothing.

    The duplication is deliberate and is the point: a hold must be able to state
    what it holds without consulting anything that can change underneath it.
    `create_reservation` snapshots the lines at hold time.
    """
    lines = reservation.lines or []
    return [(str(line.get("sku", "")), int(line.get("quantity", 0))) for line in lines]


async def reserved_units(
    session: AsyncSession, reservation: StockReservation
) -> list[tuple[str, int]]:
    """The (sku, quantity) pairs a reservation holds.

    Falls back to `order_items` for rows written before this column existed, and
    returns an empty list only when neither source has anything -- which the
    callers treat as "nothing to release", and which
    `test_a_reservation_without_lines_reports_no_units` pins so the silent case
    stays visible.
    """
    lines = reservation_lines(reservation)
    if lines:
        return lines

    from backend.database.models import OrderItem

    result = await session.execute(
        select(OrderItem.sku, OrderItem.quantity)
        .where(OrderItem.order_id == reservation.order_id)
        .order_by(OrderItem.sku)
    )
    return [(sku, int(quantity)) for sku, quantity in result.all()]
