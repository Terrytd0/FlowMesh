"""Inventory: the conditional decrement, under real concurrency.

This file is the sprint's oversell guarantee, and the tests in it are the ones
that make the resume line defensible. Three properties are asserted:

1. **Stock never goes negative**, at any concurrency.
2. **Every refusal is reported**, not silently dropped.
3. **Every release and commit is symmetric**, so the ledger reconciles.

The concurrency test fires 500 reservations at 10 units against one row. If the
conditional `UPDATE` were a read-then-write, this test would oversell and fail --
not flakily, but by a wide margin, which is what makes it a useful regression
guard rather than a probabilistic one.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from backend.database.repositories import inventory as repo
from backend.events.schema import ReservationState
from backend.inventory.service import InventoryService

WH = "WH-01"
SKU = "SKU-HEADPHONES"


@pytest.fixture
async def row(session: Any) -> Any:
    """One row with ten units available.

    Returns the row object for its side effect (ensuring it exists) rather than
    being the accessor: it is seeded here, and every test re-reads it through
    `_row` afterwards so the assertion is on the state the repository wrote rather
    than on the object the fixture happened to hold.
    """
    await repo.seed_warehouses(session, [{"id": WH, "name": "W1", "region": "europe"}])
    await repo.seed_inventory(session, [{"warehouse_id": WH, "sku": SKU, "on_hand": 10}])
    await session.commit()
    return await _row(session)


async def _row(session: Any, sku: str = SKU) -> Any:
    """`get_inventory` for a row the test has just written, asserted present.

    `get_inventory` returns `Optional` because "no such row" is a real answer, and
    every production call site handles it. In a test that has just inserted the
    row, `None` is a failure worth surfacing -- and asserting it once here rather
    than scattering `assert x is not None` after every call keeps the assertions
    about stock arithmetic readable.
    """
    stored = await repo.get_inventory(session, WH, sku)
    assert stored is not None, f"no inventory row for {WH}/{sku}"
    return stored


# ---------------------------------------------------------------- the guarantee


async def test_reserve_decrements_available(session: Any, row: Any) -> None:
    assert await repo.reserve_units(session, warehouse_id=WH, sku=SKU, quantity=3)
    await session.commit()
    refreshed = await _row(session)
    assert refreshed.available == 7
    assert refreshed.reserved == 3
    assert refreshed.on_hand == 10


async def test_release_restores_available(session: Any, row: Any) -> None:
    await repo.reserve_units(session, warehouse_id=WH, sku=SKU, quantity=4)
    assert await repo.release_units(session, warehouse_id=WH, sku=SKU, quantity=4)
    await session.commit()
    refreshed = await _row(session)
    assert refreshed.available == 10
    assert refreshed.reserved == 0


async def test_commit_moves_reserved_to_shipped(session: Any, row: Any) -> None:
    """`available` does not move on commit -- the units were already unavailable.

    Decrementing it as well is a double-count that quietly inflates apparent
    stock, and it does not show up until someone reconciles the ledger against
    the log.
    """
    await repo.reserve_units(session, warehouse_id=WH, sku=SKU, quantity=6)
    assert await repo.commit_units(session, warehouse_id=WH, sku=SKU, quantity=6)
    await session.commit()
    refreshed = await _row(session)
    assert refreshed.on_hand == 4
    assert refreshed.reserved == 0
    assert refreshed.available == 4


async def test_refuses_more_than_available(session: Any, row: Any) -> None:
    assert not await repo.reserve_units(session, warehouse_id=WH, sku=SKU, quantity=11)
    await session.commit()
    refreshed = await _row(session)
    assert refreshed.available == 10, "a refused reservation must not move stock"


async def test_refuses_a_missing_row(session: Any) -> None:
    assert not await repo.reserve_units(session, warehouse_id=WH, sku="SKU-NOPE", quantity=1)


async def test_release_beyond_reserved_is_refused(session: Any, row: Any) -> None:
    """The symmetric guard.

    An unguarded release is how stock appears from nowhere after a double
    release -- which looks exactly like an oversell on the balance sheet.
    """
    await repo.reserve_units(session, warehouse_id=WH, sku=SKU, quantity=2)
    assert not await repo.release_units(session, warehouse_id=WH, sku=SKU, quantity=5)
    await session.commit()
    refreshed = await _row(session)
    assert refreshed.available == 8, "stock appeared from nowhere"


async def test_zero_and_negative_quantities_are_rejected(session: Any, row: Any) -> None:
    with pytest.raises(ValueError, match="quantity"):
        await repo.reserve_units(session, warehouse_id=WH, sku=SKU, quantity=0)
    with pytest.raises(ValueError, match="quantity"):
        await repo.release_units(session, warehouse_id=WH, sku=SKU, quantity=-1)
    with pytest.raises(ValueError, match="quantity"):
        await repo.commit_units(session, warehouse_id=WH, sku=SKU, quantity=0)


async def test_ledger_stays_consistent_through_a_full_cycle(session: Any, row: Any) -> None:
    """`available = on_hand - reserved` holds at every step, and stock drains.

    The check constraint enforces the identity too. Asserting it here is the
    difference between "the database rejected a bad write" and "the code did the
    right thing".

    The expected values are *derived from the row* rather than hardcoded per step,
    because `commit_units` decrements `on_hand` as well as `reserved` -- so after
    five cycles of 2 units, `on_hand` is 0 and a test that asserted "reserved == 2
    per step" would be asserting a bug.
    """
    for step in range(5):
        expected_available = 10 - 2 * step
        assert await repo.reserve_units(session, warehouse_id=WH, sku=SKU, quantity=2)
        current = await _row(session)
        assert current.reserved == 2, "the hold was not reflected on the row"
        assert current.available == expected_available - 2
        assert current.available == current.on_hand - current.reserved

        assert await repo.commit_units(session, warehouse_id=WH, sku=SKU, quantity=2)
        after = await _row(session)
        assert after.reserved == 0
        assert after.on_hand == expected_available - 2
        # `available` must not move on commit: the units were already unavailable
        # while held, and decrementing again would inflate apparent stock.
        assert after.available == after.on_hand
    await session.commit()
    final = await _row(session)
    assert final.on_hand == 0
    assert final.available == 0
    assert final.reserved == 0


# ---------------------------------------------------------------- concurrency


def _fresh_session(session_factory: Any) -> Any:
    """One session from the factory, for setup and verification blocks.

    A plain function, not a coroutine function: `async_sessionmaker.__call__`
    already returns an `AsyncSession`, which is itself an async context manager.
    Wrapping it in `async def` would produce a coroutine that `async with` cannot
    enter -- and would only say so with a "coroutine was never awaited" warning at
    teardown, by which point the real failure has scrolled past.
    """
    return session_factory()


async def test_five_hundred_concurrent_reservations_never_oversell(
    concurrent_factory: Any, metrics: Any
) -> None:
    """The load-bearing test.

    500 concurrent reservations of 1 unit against 10 units. Exactly 10 must
    succeed. With a read-then-write this test oversells by a wide margin rather
    than flakily, which is what makes it a regression guard.

    Each attempt gets its *own* session, as 500 real connections would. Sharing
    one session would serialise the transactions inside the ORM and quietly
    remove the very race the test exists to probe -- which is also why this uses
    the `concurrent_factory` fixture rather than `session_factory`. That fixture's
    docstring records the measurement: with the shared-connection fixture this
    same test grants 182 of 500 and ends with `available = -172`, which says
    nothing about the SQL.

    The `metrics` argument is not used to assert a count here. It is taken so the
    fixture that builds the registry is constructed for this test like every
    other one -- a metric assertion in a concurrency test reads as if the metric
    were the thing being verified, and it is not. The oversell counter is asserted
    separately, in `test_oversell_rejections_are_counted`.
    """
    async with _fresh_session(concurrent_factory) as setup:
        await repo.seed_warehouses(setup, [{"id": WH, "name": "W1", "region": "europe"}])
        await repo.seed_inventory(setup, [{"warehouse_id": WH, "sku": SKU, "on_hand": 10}])
        await setup.commit()

    async def _attempt() -> bool:
        async with _fresh_session(concurrent_factory) as attempt_session:
            async with attempt_session.begin():
                return bool(
                    await repo.reserve_units(attempt_session, warehouse_id=WH, sku=SKU, quantity=1)
                )

    results = await asyncio.gather(*(_attempt() for _ in range(500)))
    granted = sum(1 for result in results if result)
    assert granted == 10, f"expected exactly 10 reservations, got {granted}"

    async with _fresh_session(concurrent_factory) as verify:
        final = await _row(verify)
        assert final.available == 0
        assert final.reserved == 10
        assert final.on_hand == 10


async def test_concurrent_releases_do_not_inflate_stock(concurrent_factory: Any) -> None:
    """100 releases of 1 unit against 10 reserved: exactly 10 may succeed."""
    async with _fresh_session(concurrent_factory) as setup:
        await repo.seed_warehouses(setup, [{"id": WH, "name": "W1", "region": "europe"}])
        await repo.seed_inventory(setup, [{"warehouse_id": WH, "sku": SKU, "on_hand": 10}])
        await setup.commit()
        await repo.reserve_units(setup, warehouse_id=WH, sku=SKU, quantity=10)
        await setup.commit()

    async def _release() -> bool:
        async with _fresh_session(concurrent_factory) as attempt_session:
            async with attempt_session.begin():
                return bool(
                    await repo.release_units(attempt_session, warehouse_id=WH, sku=SKU, quantity=1)
                )

    results = await asyncio.gather(*(_release() for _ in range(100)))
    assert sum(1 for result in results if result) == 10

    async with _fresh_session(concurrent_factory) as verify:
        final = await _row(verify)
        assert final.available == 10, "stock was inflated by concurrent releases"
        assert final.reserved == 0


async def test_oversell_rejections_are_counted(session: Any, metrics: Any, row: Any) -> None:
    """Refusals are visible, not silent.

    "We refused 3% of orders" is a number a supply-chain team can act on; a log
    line nobody reads is not.
    """
    service = InventoryService(session=session, metrics=metrics, ttl_seconds=900)
    outcome = await service.reserve_for_order(
        order_id="CRG-1", lines=[{"sku": SKU, "quantity": 999}]
    )
    await session.commit()
    assert not outcome.reserved
    assert metrics.value_of("flowmesh_oversell_rejections_total", warehouse=WH) == 1.0


# ---------------------------------------------------------------- reservations


async def test_reservation_row_is_created_with_a_ttl(session: Any, row: Any) -> None:
    reservation = await repo.create_reservation(
        session, order_id="CRG-1", warehouse_id=WH, ttl_seconds=900
    )
    await session.commit()
    assert reservation.state == ReservationState.HELD
    assert reservation.expires_at > reservation.created_at


async def test_a_second_reservation_for_the_same_order_and_warehouse_is_rejected(
    session: Any, row: Any
) -> None:
    """The row-level idempotency guard behind a redelivered `inventory.reserved`."""
    from sqlalchemy.exc import IntegrityError

    await repo.create_reservation(session, order_id="CRG-1", warehouse_id=WH, ttl_seconds=900)
    await session.commit()
    with pytest.raises(IntegrityError):
        await repo.create_reservation(session, order_id="CRG-1", warehouse_id=WH, ttl_seconds=900)


async def test_expired_reservations_are_found(session: Any, row: Any) -> None:
    from backend.core.clock import utcnow

    reservation = await repo.create_reservation(
        session, order_id="CRG-1", warehouse_id=WH, ttl_seconds=900
    )
    await session.commit()
    assert await repo.expired_reservations(session, now=utcnow()) == []

    later = utcnow() + timedelta(seconds=1000)
    assert len(await repo.expired_reservations(session, now=later)) == 1
    _ = reservation


async def test_setting_a_reservation_state_reports_a_missing_row(session: Any, row: Any) -> None:
    assert not await repo.set_reservation_state(
        session, reservation_id="does-not-exist", state=ReservationState.RELEASED
    )


# ---------------------------------------------------------------- the service


async def test_reserve_for_order_is_all_or_nothing(session: Any, metrics: Any) -> None:
    """Partial reservation is worse than none.

    The customer is told two of four items are coming and the other two are
    silently unavailable with no order to explain why. So a failure on any line
    rolls back the whole attempt inside the transaction.
    """
    await repo.seed_warehouses(session, [{"id": WH, "name": "W1", "region": "europe"}])
    await repo.seed_inventory(
        session,
        [
            {"warehouse_id": WH, "sku": SKU, "on_hand": 10},
            {"warehouse_id": WH, "sku": "SKU-MUG-STD", "on_hand": 1},
        ],
    )
    await session.commit()

    service = InventoryService(session=session, metrics=metrics, ttl_seconds=900)
    outcome = await service.reserve_for_order(
        order_id="CRG-1",
        lines=[
            {"sku": SKU, "quantity": 2},
            {"sku": "SKU-MUG-STD", "quantity": 5},
        ],
    )
    await session.commit()
    assert not outcome.reserved
    assert outcome.shortfalls == [("SKU-MUG-STD", 5)]

    # The line that *could* have been reserved was rolled back.
    headphones = await _row(session)
    assert headphones.available == 10, "a partial reservation was left behind"
    assert await repo.get_reservation(session, order_id="CRG-1", warehouse_id=WH) is None


async def test_reserve_picks_the_warehouse_with_the_most_headroom(
    session: Any, metrics: Any
) -> None:
    """Not the first that fits.

    Choosing greedily packs one warehouse empty and leaves the next order
    unservable -- a greedy allocator turning a distribution problem into a
    stockout problem.
    """
    await repo.seed_warehouses(
        session,
        [
            {"id": "WH-01", "name": "A", "region": "europe"},
            {"id": "WH-02", "name": "B", "region": "americas"},
        ],
    )
    await repo.seed_inventory(
        session,
        [
            {"warehouse_id": "WH-01", "sku": SKU, "on_hand": 5},
            {"warehouse_id": "WH-02", "sku": SKU, "on_hand": 500},
        ],
    )
    await session.commit()

    service = InventoryService(session=session, metrics=metrics, ttl_seconds=900)
    outcome = await service.reserve_for_order(order_id="CRG-1", lines=[{"sku": SKU, "quantity": 1}])
    await session.commit()
    assert outcome.warehouse_id == "WH-02"


async def test_reserve_reports_when_no_warehouse_has_the_item(session: Any, metrics: Any) -> None:
    await repo.seed_warehouses(session, [{"id": WH, "name": "W1", "region": "europe"}])
    await session.commit()
    service = InventoryService(session=session, metrics=metrics, ttl_seconds=900)
    outcome = await service.reserve_for_order(
        order_id="CRG-1", lines=[{"sku": "SKU-NOWHERE", "quantity": 1}]
    )
    assert not outcome.reserved
    assert "no warehouse" in outcome.reason


async def test_commit_and_release_are_noops_on_an_unknown_order(session: Any, metrics: Any) -> None:
    service = InventoryService(session=session, metrics=metrics, ttl_seconds=900)
    assert not await service.commit_reservation(order_id="ghost", warehouse_id=WH)
    assert not await service.release_reservation(order_id="ghost", warehouse_id=WH, reason="test")


async def test_the_sweeper_reclaims_an_expired_hold(session: Any, metrics: Any) -> None:
    """A hold that is never released is stock that is never sold.

    This is the failure a reservation TTL exists to prevent: an order held for
    review forever, holding its units forever, and the catalogue quietly
    underselling.
    """
    await repo.seed_warehouses(session, [{"id": WH, "name": "W1", "region": "europe"}])
    await repo.seed_inventory(session, [{"warehouse_id": WH, "sku": SKU, "on_hand": 10}])
    await session.commit()

    service = InventoryService(session=session, metrics=metrics, ttl_seconds=0)
    outcome = await service.reserve_for_order(order_id="CRG-1", lines=[{"sku": SKU, "quantity": 4}])
    assert outcome.reserved
    await session.commit()

    # The sweep runs on a later clock, as the scheduled job would. `set_clock_offset`
    # rather than monkeypatching `utcnow`: the repositories imported `utcnow` by
    # value at import time, so rebinding the attribute on `backend.core.clock`
    # would leave every module under test still calling the original -- the test
    # would pass on a reservation that had not actually expired.
    from backend.core.clock import reset_clock_offset, set_clock_offset

    set_clock_offset(timedelta(seconds=10))
    try:
        reclaimed = await service.sweep_expired()
    finally:
        reset_clock_offset()
    await session.commit()
    assert reclaimed == 1
    assert metrics.value_of("flowmesh_reservation_expirations_total") == 1.0

    refreshed = await _row(session)
    assert refreshed.available == 10
    assert refreshed.reserved == 0


async def test_a_reservation_without_lines_reports_no_units(session: Any, row: Any) -> None:
    """Pins the silent-failure case `reservation_lines` documents.

    A hold with no snapshotted lines and no order rows has nothing to release. The
    property worth locking in is that this is *visible* -- no units, rather than a
    release that reports success while moving nothing.
    """
    reservation = await repo.create_reservation(
        session, order_id="CRG-NO-LINES", warehouse_id=WH, ttl_seconds=900
    )
    await session.commit()
    assert repo.reservation_lines(reservation) == []
    assert await repo.reserved_units(session, reservation) == []


async def test_reservation_lines_are_snapshotted_at_hold_time(session: Any, metrics: Any) -> None:
    """The hold states what it covers without consulting another table.

    This is what stops a release from silently doing nothing: the units come back
    from the reservation row, which was written in the same transaction as the
    decrement.
    """
    await repo.seed_warehouses(session, [{"id": WH, "name": "W1", "region": "europe"}])
    await repo.seed_inventory(
        session,
        [
            {"warehouse_id": WH, "sku": SKU, "on_hand": 10},
            {"warehouse_id": WH, "sku": "SKU-MUG-STD", "on_hand": 10},
        ],
    )
    await session.commit()

    service = InventoryService(session=session, metrics=metrics, ttl_seconds=900)
    outcome = await service.reserve_for_order(
        order_id="CRG-2",
        lines=[
            {"sku": SKU, "quantity": 2},
            {"sku": "SKU-MUG-STD", "quantity": 3},
        ],
    )
    await session.commit()
    assert outcome.reserved

    reservation = await repo.get_reservation(session, order_id="CRG-2", warehouse_id=WH)
    assert reservation is not None
    assert sorted(repo.reservation_lines(reservation)) == [
        (SKU, 2),
        ("SKU-MUG-STD", 3),
    ]

    # And the release returns all five units with no order rows in sight.
    assert await service.release_reservation(order_id="CRG-2", warehouse_id=WH, reason="test")
    await session.commit()
    for sku in (SKU, "SKU-MUG-STD"):
        row = await _row(session, sku)
        assert row.available == 10, f"{sku} was not fully released"


async def test_total_available_sums_across_warehouses(session: Any, seeded_warehouses: Any) -> None:
    total = await repo.total_available(session, "SKU-TSHIRT-M")
    # Twelve warehouses at 500 each, from the `seeded_warehouses` fixture.
    assert total == 6000
