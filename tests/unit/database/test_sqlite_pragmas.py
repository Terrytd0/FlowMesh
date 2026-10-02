"""The SQLite pragmas must actually be applied.

`apply_sqlite_pragmas` was declared `async def` and called without `await`, so its
body never ran. Registering an event listener is synchronous work, and a coroutine
nobody awaits does nothing at all -- silently. Every engine built by `get_engine`
therefore ran with **no** WAL, **no** `foreign_keys=ON`, **no** `busy_timeout` and
no `synchronous`, while the function's own docstring described a fix that was not in
effect and attributed a hundred-odd `OperationalError: database is locked` failures
to it.

The tell was in `backend/loadtest/harness.py`, which had meanwhile grown its own
working copy of the pragmas: when the load test needed a pragma the application
engine did not have, the copy is where it went.

Two tests, because there are two different mistakes and they fail differently. One
asserts the pragmas are in force on a live connection. The other asserts the function
is not a coroutine, which is the property whose violation is invisible at every call
site -- an `async def` called without `await` returns a coroutine that is never
awaited and reports no error.
"""

from __future__ import annotations

import asyncio
import inspect
import tempfile
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from backend.database.session import apply_sqlite_pragmas


async def _probe(url: str, *, apply: bool) -> dict[str, Any]:
    """Open one connection and read back what SQLite is actually configured with."""
    engine = create_async_engine(url, connect_args={"check_same_thread": False})
    try:
        if apply:
            apply_sqlite_pragmas(engine, busy_timeout_ms=30_000)
        async with engine.connect() as connection:
            async with connection.begin():
                found: dict[str, Any] = {}
                for pragma in ("journal_mode", "foreign_keys", "busy_timeout", "synchronous"):
                    found[pragma] = (await connection.exec_driver_sql(f"PRAGMA {pragma}")).scalar()
                return found
    finally:
        await engine.dispose()


def _url() -> str:
    directory = Path(tempfile.mkdtemp(prefix="flowmesh-pragma-"))
    return "sqlite+aiosqlite:///" + (directory / "pragmas.sqlite3").as_posix()


def test_the_pragma_helper_is_not_a_coroutine_function() -> None:
    """It must be a plain function.

    The failure this guards is completely silent: `apply_sqlite_pragmas(engine)` on a
    coroutine function returns an un-awaited coroutine, warns only under
    `-W error::RuntimeWarning`, and applies nothing. Every assertion below would
    pass in a world where the function exists and does nothing, which is the world
    this project shipped.
    """
    assert not inspect.iscoroutinefunction(apply_sqlite_pragmas), (
        "apply_sqlite_pragmas is async but is called synchronously, so its body never "
        "runs and no pragma is ever applied. Make it a plain def."
    )


def test_the_pragmas_are_in_force_on_a_real_connection() -> None:
    """WAL, foreign keys, a busy timeout, and NORMAL durability.

    These are the four that matter, and each was silently absent:

    - `journal_mode=WAL`: without it every write blocks every reader, and the load
      test measures SQLite's locking rather than the pipeline;
    - `foreign_keys=ON`: off by default in SQLite, so the `order_items -> orders`
      foreign key was unenforced on every SQLite-backed run while being enforced on
      PostgreSQL -- a constraint that exists only in production is not a constraint;
    - `busy_timeout`: without it a contended write fails instantly instead of
      waiting, and SQLite has exactly one writer;
    - `synchronous=NORMAL`: the WAL-recommended setting, which is also the one that
      makes `BEGIN IMMEDIATE` queue sensibly.
    """
    pragmas = asyncio.run(_probe(_url(), apply=True))

    assert str(pragmas["journal_mode"]).lower() == "wal", pragmas
    assert pragmas["foreign_keys"] == 1, f"foreign key enforcement is off: {pragmas}"
    assert pragmas["busy_timeout"] == 30_000, pragmas
    # 1 is NORMAL, 2 is FULL.
    assert pragmas["synchronous"] == 1, f"expected synchronous=NORMAL: {pragmas}"


def test_without_the_helper_sqlite_keeps_its_insecure_defaults() -> None:
    """The control case, so the test above is measuring the helper and not SQLite.

    Without this, a future SQLite that enabled foreign keys by default would make
    `test_the_pragmas_are_in_force_on_a_real_connection` pass for the wrong reason --
    and the whole bug was that the configuration was assumed rather than observed.
    """
    pragmas = asyncio.run(_probe(_url(), apply=False))

    assert pragmas["foreign_keys"] == 0, (
        "SQLite now defaults foreign_keys to ON; this control no longer proves the "
        f"helper does anything: {pragmas}"
    )


async def _assert_begin_immediate(url: str) -> None:
    engine = create_async_engine(url, connect_args={"check_same_thread": False})
    try:
        apply_sqlite_pragmas(engine)
        statements: list[str] = []

        from sqlalchemy import event

        @event.listens_for(engine.sync_engine, "before_cursor_execute")
        def _record(_conn, _cursor, statement, _params, _context, _many):  # type: ignore[no-untyped-def]
            statements.append(" ".join(statement.split()))

        async with engine.begin() as connection:
            await connection.execute(text("SELECT 1"))

        assert any("BEGIN IMMEDIATE" in s for s in statements), (
            "no BEGIN IMMEDIATE was emitted, so write transactions are deferred and a "
            f"read-then-write will fail instantly under contention: {statements}"
        )
    finally:
        await engine.dispose()


def test_transactions_begin_immediate_rather_than_deferred() -> None:
    """`BEGIN IMMEDIATE`, which is what turns a lock conflict into a wait.

    A DEFERRED transaction takes a SHARED lock on its first read and needs to
    upgrade to RESERVED to write. SQLite cannot wait out that upgrade without risking
    a deadlock against itself -- two readers each holding SHARED, each waiting for the
    other -- so it returns SQLITE_BUSY *immediately* and never consults the busy
    handler. `busy_timeout` therefore does nothing for the transactions this pipeline
    actually writes, which are read-then-write.

    The chaos test is what made it unavoidable: it killed a consumer mid-stream and
    the replacement's first statement -- the idempotency INSERT on a fresh
    connection -- hit `database is locked`, burned three retries in about 150ms, and
    took the subscription down with it.
    """
    asyncio.run(_assert_begin_immediate(_url()))


async def _assert_in_memory_is_tolerated(url: str) -> None:
    engine = create_async_engine(url, connect_args={"check_same_thread": False})
    try:
        apply_sqlite_pragmas(engine)
        async with engine.connect() as connection:
            async with connection.begin():
                busy_timeout = (await connection.exec_driver_sql("PRAGMA busy_timeout")).scalar()
                assert busy_timeout is not None and busy_timeout >= 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize("dialect_url", ["sqlite+aiosqlite:///:memory:"])
def test_the_helper_tolerates_an_in_memory_database(dialect_url: str) -> None:
    """It must not raise on any SQLite URL the suite builds.

    The test fixtures use `sqlite:///:memory:`, so a pragma helper that only works
    against a file would leave the whole unit suite on a different configuration from
    the one the load test exercises -- which is exactly how the two drifted apart.
    """
    asyncio.run(_assert_in_memory_is_tolerated(dialect_url))
