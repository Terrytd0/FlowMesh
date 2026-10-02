"""Seed the twelve warehouses, a stock catalogue, and demo users.

Idempotent. `make up` runs it against a database that may already have data, and
a seed that fails halfway leaves a project in a state nobody wants to debug -- so
every write here is a skip-if-present or an explicit overwrite.

`--overwrite-stock` resets inventory to the seeded quantities. That is the flag
you want after a load test, and the reason it is explicit: a seed script that
silently resets stock is a seed script that will eventually erase real inventory
somebody was looking at.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from backend.config.settings import get_settings
from backend.core.logging import configure_logging, get_logger
from backend.database.repositories import inventory as repo
from backend.database.session import create_all, get_engine, session_scope

logger = get_logger(__name__)

#: The client's twelve fulfilment centres. Region matters -- the shortfall handler
#: uses `ships_international`, and region is what a support agent asks first when
#: an order cannot be served.
WAREHOUSES: list[dict[str, Any]] = [
    {
        "id": "WH-01",
        "name": "Rotterdam DC",
        "region": "europe",
        "ships_international": True,
    },
    {"id": "WH-02", "name": "Halle DC", "region": "europe", "ships_international": True},
    {"id": "WH-03", "name": "Lyon DC", "region": "europe", "ships_international": True},
    {"id": "WH-04", "name": "Manchester DC", "region": "europe", "ships_international": True},
    {"id": "WH-05", "name": "Newark DC", "region": "americas", "ships_international": True},
    {"id": "WH-06", "name": "Dallas DC", "region": "americas", "ships_international": True},
    {"id": "WH-07", "name": "Chicago DC", "region": "americas", "ships_international": False},
    {"id": "WH-08", "name": "Atlanta DC", "region": "americas", "ships_international": False},
    {"id": "WH-09", "name": "Toronto DC", "region": "americas", "ships_international": True},
    {"id": "WH-10", "name": "Singapore DC", "region": "apac", "ships_international": True},
    {"id": "WH-11", "name": "Sydney DC", "region": "apac", "ships_international": True},
    {"id": "WH-12", "name": "Osaka DC", "region": "apac", "ships_international": False},
]

SKUS: list[dict[str, Any]] = [
    {"sku": "SKU-TSHIRT-M", "name": "Heavyweight T-Shirt (M)", "price_cents": 2400},
    {"sku": "SKU-TSHIRT-L", "name": "Heavyweight T-Shirt (L)", "price_cents": 2400},
    {"sku": "SKU-MUG-STD", "name": "Stoneware Mug", "price_cents": 1200},
    {"sku": "SKU-HEADPHONES", "name": "Wireless Headphones", "price_cents": 18900},
    {"sku": "SKU-KETTLE-PRO", "name": "Pro Kettle", "price_cents": 6500},
    {"sku": "SKU-DESK-LAMP", "name": "Desk Lamp", "price_cents": 4200},
    {"sku": "SKU-RARE-EDITION", "name": "Numbered Print", "price_cents": 45000},
]

#: Stock per warehouse per SKU. Uniform except for the numbered print, which is
#: scarce in exactly one warehouse -- so the shortfall path is exercisable
#: without arranging a shortage by hand.
STOCK: dict[str, int] = {
    "SKU-TSHIRT-M": 500,
    "SKU-TSHIRT-L": 400,
    "SKU-MUG-STD": 900,
    "SKU-HEADPHONES": 150,
    "SKU-KETTLE-PRO": 220,
    "SKU-DESK-LAMP": 300,
}


async def seed(*, overwrite_stock: bool = False) -> dict[str, int]:
    """Insert everything that is missing. Returns what was written."""
    settings = get_settings()
    if settings.storage_kind == "sqlite":
        await create_all(get_engine(settings))

    async with session_scope(settings) as session:
        warehouses_written = await repo.seed_warehouses(session, WAREHOUSES)
        rows = [
            {"warehouse_id": warehouse["id"], "sku": sku, "on_hand": quantity}
            for warehouse in WAREHOUSES
            for sku, quantity in STOCK.items()
        ]
        rows.append({"warehouse_id": "WH-01", "sku": "SKU-RARE-EDITION", "on_hand": 3})
        stock_written = await repo.seed_inventory(session, rows, overwrite=overwrite_stock)

    return {"warehouses": warehouses_written, "stock": stock_written}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Seed FlowMesh development data.")
    parser.add_argument(
        "--overwrite-stock",
        action="store_true",
        help="reset inventory to the seeded quantities (destructive)",
    )
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_level_json)
    written = await seed(overwrite_stock=args.overwrite_stock)
    logger.info("seed complete: %s", written)

    if written == {"warehouses": 0, "stock": 0}:
        logger.info("nothing to do -- the database is already seeded")


if __name__ == "__main__":
    asyncio.run(main())
