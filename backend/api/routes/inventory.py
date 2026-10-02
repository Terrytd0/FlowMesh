"""Inventory and warehouse reads.

Read-only on purpose. Every stock mutation goes through the event pipeline -- an
`inventory.reserved` event published by the order processor, applied by the
inventory worker -- and nothing in the API writes stock directly.

That restriction is worth defending, because "just update the row in the API"
looks like a shortcut until you ask what happens when two requests do it at once
and when the log no longer explains the ledger. The API's job here is to *show*
the ledger; the pipeline's job is to change it.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import select

from backend.api.schemas import InventoryRowOut, WarehouseOut
from backend.auth.dependencies import CurrentPrincipal, DbSession
from backend.database.models import Warehouse
from backend.database.repositories import inventory as repo

router = APIRouter(tags=["inventory"])


@router.get("/inventory", response_model=list[InventoryRowOut])
async def list_inventory(
    session: DbSession,
    principal: CurrentPrincipal,
    sku: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
) -> list[InventoryRowOut]:
    """Stock by warehouse, optionally for one SKU.

    `available` is the number that matters to a customer; `reserved` and
    `on_hand` are what an operator needs to explain a discrepancy. All three, so
    the dashboard does not have to join to tell "sold out" from "reserved by
    someone else".
    """
    _ = principal
    rows = await repo.list_inventory(session, sku=sku)
    return [
        InventoryRowOut(
            warehouse_id=row.warehouse_id,
            sku=row.sku,
            on_hand=row.on_hand,
            reserved=row.reserved,
            available=row.available,
            version=row.version,
        )
        for row in rows
    ]


@router.get("/inventory/{warehouse_id}/{sku}", response_model=InventoryRowOut)
async def get_inventory_row(
    warehouse_id: str, sku: str, session: DbSession, principal: CurrentPrincipal
) -> InventoryRowOut:
    _ = principal
    row = await repo.get_inventory(session, warehouse_id, sku)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="no such inventory row")
    return InventoryRowOut(
        warehouse_id=row.warehouse_id,
        sku=row.sku,
        on_hand=row.on_hand,
        reserved=row.reserved,
        available=row.available,
        version=row.version,
    )


@router.get("/warehouses", response_model=list[WarehouseOut])
async def list_warehouses(session: DbSession, principal: CurrentPrincipal) -> list[WarehouseOut]:
    _ = principal
    result = await session.execute(select(Warehouse).order_by(Warehouse.id))
    return [
        WarehouseOut(
            id=row.id,
            name=row.name,
            region=row.region,
            ships_international=row.ships_international,
        )
        for row in result.scalars()
    ]


@router.get("/inventory/total/{sku}", response_model=dict[str, Any])
async def total_available(
    sku: str, session: DbSession, principal: CurrentPrincipal
) -> dict[str, Any]:
    """Total available for a SKU across every warehouse.

    The number the shortfall handler reasons about, exposed so an operator can
    check it by hand: when an order was refused, this is what it was refused
    against.
    """
    _ = principal
    total = await repo.total_available(session, sku)
    return {"sku": sku, "total_available": total}
