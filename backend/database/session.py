"""Async engine and session factory.

Loop-scoped, not process-wide, and that is a hard requirement rather than a
style preference. SQLAlchemy's async engine holds pooled connections bound to
whichever event loop first checked them out. This application legitimately runs
two long-lived loops -- FastAPI's request-handling loop, and the consumer loops in
`backend/pipeline/` -- so a single module-level engine shared by both eventually
hands a connection created on loop A to a caller on loop B. That raises
`RuntimeError` at best and, worse, leaves a poisoned connection in the pool that
a later unrelated request draws and 500s on.

`get_engine()` keys the engine by the running loop for exactly that reason.

The SQLite branch is not a convenience. It is what lets the load test, the chaos
test and the entire unit suite execute the *real* SQL -- including the conditional
`UPDATE ... WHERE available >= :qty` that prevents oversell -- with no server
running. Same models, same statements, two dialects; see docs/architecture.md
section 4 for the one place the dialects genuinely differ.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.config.settings import Settings, get_settings
from backend.core.logging import get_logger

# Importing the models module registers every table on `Base.metadata`.
#
# **This import is load-bearing, and its absence is completely silent.**
# `Base.metadata` starts empty, so `create_all()` creates *no tables at all*,
# `alembic revision --autogenerate` proposes dropping the entire schema, and the
# first query fails with `no such table: orders`. Every call succeeds; the database
# is simply empty. Found by the API tests, which created their schema and then
# found no tables in it.
from backend.database import models as _models_registered  # noqa: F401
from backend.database.base import Base

logger = get_logger(__name__)

#: Every table this application owns. Read by `alembic/env.py` and asserted in
#: tests, so a table that fails to register is visible rather than inferred from a
#: query error later.
KNOWN_TABLES: frozenset[str] = frozenset(Base.metadata.tables)

assert KNOWN_TABLES, (
    "no tables registered on Base.metadata -- backend.database.models must be "
    "imported before create_all(), or the schema is silently created empty"
)

_engines: dict[asyncio.AbstractEventLoop, AsyncEngine] = {}
_factories: dict[asyncio.AbstractEventLoop, async_sessionmaker[AsyncSession]] = {}
_lock = asyncio.Lock()


def _loop_key() -> asyncio.AbstractEventLoop:
    """The running loop, which is the natural cache key for an async engine."""
    return asyncio.get_running_loop()


def engine_kwargs(settings: Settings) -> dict[str, object]:
    """Dialect-appropriate engine arguments.

    The one place the two databases differ in construction. `pool_size` and
    `max_overflow` are meaningless for SQLite's driver and rejected by SQLAlchemy
    if passed, so they are gated on the dialect rather than sprinkled through the
    call sites -- and the gate is `settings.storage_kind`, a single property.
    """
    if settings.storage_kind == "sqlite":
        return {
            # A file-backed SQLite database with 500 orders/sec writing needs
            # WAL, or every write blocks every reader and the load test measures
            # SQLite's locking rather than the pipeline.
            "connect_args": {"check_same_thread": False, "timeout": 30},
            "pool_pre_ping": True,
        }
    return {
        "pool_size": settings.db_pool_size,
        "max_overflow": settings.db_max_overflow,
        "pool_pre_ping": True,
        # `statement_cache_size=0` disables the prepared-statement cache that
        # asyncpg keeps keyed on the query text. At 500 orders/sec with a handful
        # of distinct statements it is pure overhead, and it is a common source
        # of "connection is closed" errors when a pool hands a connection to a
        # loop that did not create it.
        "connect_args": {"statement_cache_size": 0},
    }


def get_engine(settings: Settings | None = None) -> AsyncEngine:
    """Return the engine for the currently running loop, creating it once."""
    resolved = settings or get_settings()
    loop = _loop_key()
    engine = _engines.get(loop)
    if engine is None:
        engine = create_async_engine(
            resolved.database_url,
            echo=resolved.db_echo,
            future=True,
            **engine_kwargs(resolved),
        )
        if resolved.storage_kind == "sqlite":
            apply_sqlite_pragmas(engine)
        _engines[loop] = engine
        logger.debug("created %s async engine for this event loop", resolved.storage_kind)
    return engine


def apply_sqlite_pragmas(
    engine: AsyncEngine, *, busy_timeout_ms: int = 30_000, synchronous: str = "NORMAL"
) -> None:
    """Make SQLite behave like a real database for this workload. One place.

    Registered once per engine, via event listeners rather than per call, so a
    connection the pool opens later gets the same settings as the first one.

    **This is a plain `def`, and it must stay one.** It was `async def` and called
    without `await`, so the body never ran: registering an event listener is
    synchronous work, and a coroutine that nobody awaits does nothing at all. The
    pragmas were therefore never applied on *any* engine built by `get_engine` --
    no WAL, no `foreign_keys=ON`, no `busy_timeout`, no `synchronous`. The
    docstring underneath this one described a fix that was not in effect, and
    `backend/loadtest/harness.py` had meanwhile grown its own working copy, which
    is the tell: when the load test needed a pragma the application engine did not
    have, the copy was where it went.

    `foreign_keys=ON` in particular is *off* by default in SQLite, so the
    `order_items -> orders` FK was unenforced on every SQLite-backed run while
    being enforced on PostgreSQL -- a constraint that only exists in production is
    not a constraint.

    **The `BEGIN IMMEDIATE` listener is the load-bearing part.** WAL gives SQLite
    many readers and one writer, and the second writer does not queue -- it fails
    immediately with `database is locked`. `busy_timeout` is meant to turn that into
    a wait, and for a transaction that opens with a write it works. It does *not*
    work for a transaction that opens with a **read**, and that is the shape of most
    of this pipeline's transactions: `mark_processed` runs inside a savepoint on a
    session whose transaction SQLAlchemy has already begun as DEFERRED, so the
    transaction picks up a SHARED lock on its first read and then needs to upgrade
    to RESERVED to write. SQLite cannot wait out that upgrade -- doing so could
    deadlock against itself, where two readers each hold SHARED and each wait for
    the other to drop it -- so it returns SQLITE_BUSY *immediately* and the busy
    handler is never consulted.

    That is why the failure looked unrelated to concurrency tuning: the pragmas were
    all set, the timeout was 30 seconds, and the error still arrived in under a
    millisecond. It is a property of *when* the transaction takes its lock, not of
    how long it is willing to wait for it.

    `BEGIN IMMEDIATE` takes the RESERVED lock up front, before any statement runs, so
    a second writer queues on `busy_timeout` exactly as intended. The
    `isolation_level = None` line alongside it hands transaction control to
    SQLAlchemy rather than to the `sqlite3` driver's implicit `BEGIN`, which is what
    makes the explicit `BEGIN IMMEDIATE` reachable at all.

    The chaos test is what made this unavoidable: it kills a consumer mid-stream and
    restarts it, and the replacement's very first statement -- the idempotency
    INSERT on a fresh connection -- hit `database is locked`, burned three retries
    in ~150ms, and took the subscription down with it.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection: Any, _record: Any) -> None:
        # Hand transaction control to SQLAlchemy; see the docstring.
        dbapi_connection.isolation_level = None
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute(f"PRAGMA synchronous={synchronous}")
        # Generous on purpose. The alternative is the handler crashing and the event
        # being redelivered, which costs more than waiting, and the wait is bounded
        # so a genuine deadlock still surfaces.
        cursor.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
        cursor.close()

    @event.listens_for(engine.sync_engine, "begin")
    def _begin_immediate(connection: Any) -> None:
        connection.exec_driver_sql("BEGIN IMMEDIATE")


def get_session_factory(
    settings: Settings | None = None, *, engine: AsyncEngine | None = None
) -> async_sessionmaker[AsyncSession]:
    """Return the session factory for the currently running loop.

    `engine` overrides the cached engine. It exists for exactly one situation: a
    process that must talk to a database *other* than the configured one -- the API
    test's lifespan, which shares the fixture's in-memory database rather than
    dialling `settings.database_url`. Without it there is no way for a caller to
    say "this session factory is for that database", and the failure mode is a
    `socket.gaierror` against a hostname that only exists inside Docker.
    """
    loop = _loop_key()
    factory = _factories.get(loop)
    if factory is None or engine is not None:
        factory = async_sessionmaker(
            bind=engine if engine is not None else get_engine(settings),
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
        if engine is None:
            _factories[loop] = factory
    return factory


@asynccontextmanager
async def transaction(settings: Settings | None = None) -> AsyncIterator[AsyncSession]:
    """A session with commit-on-success and rollback-on-error.

    The single place that decides what "the handler finished cleanly" means, so
    there is exactly one implementation of the commit for every caller: the API
    routes, the consumers, the workers and the scripts.

    It does *not* suppress exceptions. A handler that fails must leave its
    transaction rolled back so the event is redelivered -- an event consumed,
    committed, and then found to have failed is data loss, and it is the specific
    failure mode `scripts/chaos_test.py` exists to rule out.
    """
    factory = get_session_factory(settings)
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_session() -> AsyncGenerator[AsyncSession]:
    """FastAPI dependency yielding a request-scoped session.

    A thin generator over `transaction`. Declared as an `AsyncGenerator` rather
    than an `AsyncIterator` because callers that manage the lifetime themselves
    must be able to close it, and a bare `AsyncIterator` has no such method.
    """
    async with transaction() as session:
        yield session


def set_default_engine(engine: AsyncEngine | None) -> None:
    """Override the process-wide default engine, for this event loop.

    The one supported way for an embedding process -- the API under `TestClient`,
    `scripts/smoke_e2e.py` -- to point the app at a database the settings do not
    name. Call it once, inside the lifespan, before any request is served.

    Passing `None` restores the settings-derived default, which is what a test's
    teardown needs; leaving a test's engine installed is how the *next* test in the
    same process silently reads the previous test's data.
    """
    loop = _loop_key()
    if engine is None:
        _engines.pop(loop, None)
        _factories.pop(loop, None)
        return
    _engines[loop] = engine
    _factories.pop(loop, None)


@asynccontextmanager
async def session_scope(settings: Settings | None = None) -> AsyncGenerator[AsyncSession]:
    """Standalone session context manager for non-request callers."""
    async with transaction(settings) as session:
        yield session


async def create_all(engine: AsyncEngine | None = None) -> None:
    """Create every table, and verify that it created something.

    The assertion is the point. `Base.metadata` is empty until `backend.database.models`
    is imported, and `create_all()` against empty metadata is a **silent no-op** --
    it succeeds, creates nothing, and the first query fails with
    `no such table: orders`. This function refuses to return in that state, which
    turns a confusing runtime error into an import-time one.
    """
    target = engine or get_engine()
    if not Base.metadata.tables:
        raise RuntimeError(
            "Base.metadata has no tables; backend.database.models must be imported "
            "before create_all(). This module imports it at the top -- if you are "
            "calling create_all from somewhere that bypasses this module, import "
            "backend.database.models yourself."
        )
    async with target.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    logger.debug("created %s tables", len(Base.metadata.tables))


async def dispose_engines() -> None:
    """Close and forget every engine, on every loop."""
    async with _lock:
        for engine in list(_engines.values()):
            await engine.dispose()
        _engines.clear()
        _factories.clear()
    logger.debug("disposed all async engines")


async def drop_all(engine: AsyncEngine | None = None) -> None:
    """Drop every table. Tests and the load test's scratch database only."""
    target = engine or get_engine()
    async with target.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)


def engine_for_url(url: str, **overrides: Any) -> AsyncEngine:
    """An uncached engine for a specific URL.

    Deliberately *not* cached, unlike `get_engine`. `get_engine` exists to hand one
    pooled engine to one event loop for the life of a process; this exists for
    callers with their own lifetime -- the API tests, which need a second
    connection to a database another fixture already created, and the integration
    tests, which point at a scratch database.

    The distinction matters for the SQLite in-memory case: two engines on
    `sqlite:///:memory:` are two *different* databases, so a caller wanting a
    second connection must pass the same URL as a file-backed one, which is why the
    API-test fixture binds `str(db_engine.url)` rather than re-deriving it.
    """
    return create_async_engine(url, future=True, **overrides)
