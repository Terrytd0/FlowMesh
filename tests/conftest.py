"""Shared test fixtures.

Four rules this suite follows, and the reason for each:

1. **`pytest` with no flags and no services running must pass.** Every
   infrastructure dependency has an in-process implementation (the event log, the
   task queue, the scorer, SQLite) that tests use by default. Integration tests
   that need a real PostgreSQL, a real Kafka or a real RabbitMQ live in
   `tests/integration/` under the `integration` marker and skip themselves
   rather than fail when the service is up. A suite that fails because a
   container is down is a suite people stop running.

2. **The suite never depends on ambient network state.** `event_transport`,
   `queue_transport` and `fraud_transport` are pinned to the in-process
   implementations here. Without that, a developer with `make up` running would
   have some tests silently switch to real infrastructure mid-run.

3. **No test module basename is reused across directories.** `tests/` has no
   `__init__.py`, so pytest's rootdir-based module naming collides between
   `tests/unit/fraud/test_model.py` and `tests/integration/test_model.py`. Every
   test filename is unique across the tree for that reason.

4. **SQLite, in memory, per test.** A file-backed database leaks state between
   tests in ways that only show up in CI. `StaticPool` with a shared in-memory
   connection makes the whole suite one transaction's worth of state, torn down
   per test.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

# Set before any application module is imported, because `get_settings()` is
# lru_cached and every fixture reads it.
os.environ.setdefault("FLOWMESH_APP_ENV", "test")
os.environ.setdefault("FLOWMESH_LOG_LEVEL", "WARNING")
os.environ.setdefault("FLOWMESH_EVENT_TRANSPORT", "memory")
os.environ.setdefault("FLOWMESH_QUEUE_TRANSPORT", "memory")
os.environ.setdefault("FLOWMESH_FRAUD_TRANSPORT", "in_process")
os.environ.setdefault("FLOWMESH_LLM_ENABLED", "false")
os.environ.setdefault("FLOWMESH_AUTH_ENABLED", "false")
os.environ.setdefault("FLOWMESH_METRICS_ENABLED", "true")

import sqlalchemy as sa  # noqa: E402 - must follow the env setup above
from prometheus_client import CollectorRegistry  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from backend.config.settings import get_settings, reload_settings  # noqa: E402
from backend.database.base import Base  # noqa: E402 - registers the schema


@pytest.fixture(autouse=True)
def _isolated_settings() -> Iterator[None]:
    """Reset the settings and metrics singletons around every test.

    Both are process-wide and cached. Without this, a test that changes a setting
    silently changes every test that runs after it -- which is the most common
    source of "passes alone, fails in the suite".
    """
    from backend.observability.metrics import reset_metrics

    reload_settings()
    reset_metrics()
    yield
    reload_settings()
    reset_metrics()


@pytest.fixture
def settings() -> Any:
    return get_settings()


@pytest.fixture
def metrics() -> Any:
    """A `FlowMeshMetrics` on its own registry.

    A fresh registry per test is what makes `value_of` able to assert an exact
    value. Registering the same names twice in the default registry raises
    `Duplicated timeseries`, which is why the singleton exists at all and why
    tests do not use it.
    """
    from backend.observability.metrics import FlowMeshMetrics

    return FlowMeshMetrics(registry=CollectorRegistry())


@pytest.fixture
async def engine() -> AsyncIterator[Any]:
    """An in-memory SQLite engine with the real schema.

    Real SQL, real constraints, no server. The `CheckConstraint`s -- including
    `available = on_hand - reserved` -- are enforced here exactly as they are on
    PostgreSQL, so a test that oversells fails in both.
    """
    test_engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )

    # WAL is meaningless for `:memory:` but the FK pragma is not: SQLite defaults
    # it off, and an unenforced foreign key in tests is an unenforced FK in the
    # test suite that would have caught a cascade bug.
    @sa.event.listens_for(test_engine.sync_engine, "connect")
    def _pragmas(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with test_engine.begin() as connection:
        # `backend.database.session` is imported for its *side effect*: it imports
        # `backend.database.models`, which registers every table on `Base.metadata`.
        #
        # `Base.metadata` starts empty, so `create_all` against it is a silent
        # no-op -- it succeeds, creates nothing, and the first query fails with
        # `no such table: orders`. Importing `base` alone is not enough, which is
        # what this fixture did originally.
        from backend.database import models as _models  # noqa: F401
        from backend.database.session import KNOWN_TABLES

        assert KNOWN_TABLES, "the models module registered no tables"
        await connection.run_sync(Base.metadata.create_all)
        tables = await connection.run_sync(
            lambda sync_connection: sync_connection.execute(
                sa.text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
        )
        created = {row[0] for row in tables} - {"sqlite_sequence"}
        assert KNOWN_TABLES <= created, (
            f"schema is missing {sorted(KNOWN_TABLES - created)}; created {sorted(created)}"
        )
    try:
        yield test_engine
    finally:
        await test_engine.dispose()


@pytest.fixture
def db_engine(engine: Any) -> Any:
    """Alias for `engine`, named for its role in the API fixtures.

    `engine` is a misleading name in `test_lifespan`: what that fixture needs is
    not "an engine" but "the database the app and the assertions must share". The
    alias makes the intent legible at the dependency line, where the alternative is
    a reader wondering whether the app opens a *second* database.
    """
    return engine


@pytest.fixture
async def session(engine: Any) -> AsyncIterator[Any]:
    """A session bound to the test engine."""
    factory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
    async with factory() as test_session:
        yield test_session


@pytest.fixture
async def concurrent_factory(tmp_path: Any) -> AsyncIterator[Any]:
    """A session factory with a *real* connection pool, for concurrency tests.

    Separate from the `engine` fixture on purpose, and the difference is the whole
    point of this fixture.

    `sqlite:///:memory:` with `StaticPool` hands every session **the same
    connection**. That is what makes the ordinary tests fast and isolated, and it
    also makes them incapable of expressing concurrency: 500 "concurrent"
    transactions queue up behind one connection and interleave inside a single
    transaction, so a test that asserts "exactly 10 of 500 reservations succeed"
    passes or fails for reasons that have nothing to do with the conditional
    decrement. Measured: it grants 182 of 500, and the row ends with
    `available = -172`.

    A file-backed database with a normal pool gives each session its own
    connection, which is the situation the SQL is actually written for -- and
    there the same test grants exactly 10. SQLite serialises the writes, which is
    the same guarantee a row lock gives on PostgreSQL.

    So: use `engine`/`session_factory` for everything sequential, and this for
    anything whose point is that two writers raced.
    """
    url = "sqlite+aiosqlite:///" + str((tmp_path / "concurrent.sqlite3").as_posix())
    concurrent_engine = create_async_engine(
        url,
        connect_args={"check_same_thread": False, "timeout": 60},
        future=True,
    )

    @sa.event.listens_for(concurrent_engine.sync_engine, "connect")
    def _pragmas(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        # WAL so a reader is not blocked by the writer, and a real busy timeout so
        # a contended write waits instead of raising "database is locked" -- the
        # production equivalent is a lock wait timeout, and both are better than a
        # test that fails on contention rather than on correctness.
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=60000")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with concurrent_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield async_sessionmaker(bind=concurrent_engine, expire_on_commit=False, autoflush=False)
    finally:
        await concurrent_engine.dispose()


@pytest.fixture
async def session_factory(engine: Any) -> Any:
    """The session factory, for components that take one (consumers, workers)."""
    return async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


@pytest.fixture
async def seeded_warehouses(session: Any) -> dict[str, Any]:
    """Twelve warehouses and a small stock catalogue, as the seed script does it.

    Twelve because that is the client's number and the load test asserts on
    per-warehouse behaviour. A fixture with three warehouses would let a bug that
    only appears at twelve through.
    """
    from backend.database.repositories import inventory as repo

    rows = [
        {
            "id": f"WH-{index:02d}",
            "name": f"Cascade Warehouse {index:02d}",
            "region": ["europe", "americas", "apac"][index % 3],
            "ships_international": index % 4 != 0,
        }
        for index in range(1, 13)
    ]
    await repo.seed_warehouses(session, rows)

    stock = [
        {"warehouse_id": warehouse["id"], "sku": sku, "on_hand": 500}
        for warehouse in rows
        for sku in ("SKU-TSHIRT-M", "SKU-MUG-STD", "SKU-HEADPHONES", "SKU-KETTLE-PRO")
    ]
    # One deliberately scarce SKU, so the shortfall path is exercisable without
    # having to arrange a shortage by hand in every test that needs one.
    stock.append({"warehouse_id": "WH-01", "sku": "SKU-RARE-EDITION", "on_hand": 3})
    await repo.seed_inventory(session, stock)
    await session.commit()
    return {"warehouses": rows, "stock": stock}


@pytest.fixture
def event_bus(metrics: Any) -> Any:
    """A fresh in-process event log.

    Built directly rather than through `create_event_bus`, so a test that wants a
    *specific* partition count gets it and does not depend on the setting.
    """
    from backend.events.memory_log import InMemoryEventLog

    return InMemoryEventLog(metrics=metrics, partitions=6)


@pytest.fixture
async def task_queue(metrics: Any) -> AsyncIterator[Any]:
    """A fresh in-process task queue, started."""
    from backend.queues.memory_queue import InMemoryTaskQueue

    queue = InMemoryTaskQueue(metrics=metrics)
    await queue.start()
    yield queue
    await queue.stop()


@pytest.fixture
def scoring_engine(settings: Any, metrics: Any) -> Any:
    """A real `FraudScoringEngine` with the real weights file.

    Real, because the interesting properties here *are* the model's: which orders
    land in which band, and that velocity is counted after the decision. A stub
    scorer would test the plumbing and none of the policy.
    """
    from backend.fraud.engine import FraudScoringEngine

    return FraudScoringEngine(settings, metrics)


@pytest.fixture
def degraded_engine(settings: Any, metrics: Any) -> Any:
    """An engine forced into the degraded path, for the fallback-policy tests."""
    from backend.fraud.engine import FraudScoringEngine
    from backend.fraud.model import HeuristicFallback

    return FraudScoringEngine(
        settings, metrics, model=HeuristicFallback().as_model(), degraded=True
    )


@pytest.fixture
def fraud_client(scoring_engine: Any, metrics: Any) -> Any:
    """The in-process scorer client -- the same code path as `grpc`, minus the wire."""
    from backend.grpc_service.client import InProcessFraudClient

    return InProcessFraudClient(scoring_engine, metrics=metrics)


@pytest.fixture
def principal_supervisor() -> Any:
    from backend.auth.dependencies import Principal

    return Principal(subject="SUP-1", role="supervisor", tenant="cascade-retail")


@pytest.fixture
def principal_customer() -> Any:
    from backend.auth.dependencies import Principal

    return Principal(subject="CUST-1", role="customer", tenant="cascade-retail")


@pytest.fixture
def test_lifespan(
    settings: Any,
    event_bus: Any,
    task_queue: Any,
    scoring_engine: Any,
    metrics: Any,
    db_engine: Any,
) -> Any:
    """A lifespan that binds the in-process dependencies instead of real ones.

    The real `lifespan` in `backend/main.py` dials Kafka, RabbitMQ and gRPC; a
    test that used it would need all three running, which violates rule 1. This
    populates exactly the same `app.state` attributes with in-process
    implementations -- so the routes find what they find in production, and a
    route that reads an attribute the lifespan does not set still fails here.

    The `db_engine` dependency is load-bearing, not incidental. Without it the app
    builds its own engine from `settings.database_url`, which names a Docker
    Compose service (`postgres:5432`), and every request dies with
    `socket.gaierror: getaddrinfo failed postgres`. Binding the same engine the
    test's fixtures write to is what makes the two sides the same database.
    """
    from contextlib import asynccontextmanager

    from backend.api.ratelimit import InMemoryIdempotencyCache, InMemoryRateLimiter
    from backend.auth.dependencies import AuthService
    from backend.database.session import set_default_engine
    from backend.grpc_service.client import InProcessFraudClient

    @asynccontextmanager
    async def _lifespan(application: Any) -> AsyncIterator[None]:
        # Point the app's database access at the fixture's engine, for the same
        # reason the fixture depends on it at all -- see the docstring above.
        # `set_default_engine(None)` on the way out, so a stale engine from a
        # previous test cannot survive into the next one.
        set_default_engine(db_engine)
        application.state.settings = settings
        application.state.metrics = metrics
        application.state.event_bus = event_bus
        application.state.task_queue = task_queue
        application.state.scorer = InProcessFraudClient(scoring_engine, metrics=metrics)
        application.state.scorer_engine = scoring_engine
        application.state.rate_limiter = InMemoryRateLimiter(
            limit_per_minute=settings.rate_limit_per_minute
        )
        application.state.idempotency_cache = InMemoryIdempotencyCache()
        application.state.auth = AuthService(settings)
        application.state.event_transport = "memory"
        application.state.queue_transport = "memory"
        application.state.fraud_transport = "in_process"
        application.state.model_version = scoring_engine.model.version
        application.state.degraded = scoring_engine.degraded
        try:
            yield
        finally:
            set_default_engine(None)

    return _lifespan


@pytest.fixture
def auth_enabled_lifespan(test_lifespan: Any, monkeypatch: Any) -> Any:
    """`test_lifespan` with authentication actually on.

    The suite default is `FLOWMESH_AUTH_ENABLED=false`, which makes
    `get_principal` return an anonymous supervisor so that most tests are not about
    auth. That default is precisely what makes an *authorization* test impossible
    against `api_client` -- every request is privileged, so every order is
    readable and the test passes vacuously.

    Re-enabling it here rather than globally keeps the two concerns separate: one
    fixture for "does this endpoint work", one for "does this endpoint refuse the
    wrong caller". The first version of the authorization test used `api_client`
    and asserted `status_code in (401, 404)` -- which a 200 passes, because 200 is
    neither.
    """
    from backend.config.settings import reload_settings

    monkeypatch.setenv("FLOWMESH_AUTH_ENABLED", "true")
    reload_settings()
    yield test_lifespan
    reload_settings()


@pytest.fixture
def authenticated_client(auth_enabled_lifespan: Any) -> Any:
    """A `TestClient` with real auth, for authorization tests."""
    from fastapi.testclient import TestClient

    from backend.config.settings import get_settings
    from backend.main import create_app

    app = create_app(get_settings())
    app.router.lifespan_context = auth_enabled_lifespan  # type: ignore[method-assign]
    with TestClient(app) as client:
        yield client


@pytest.fixture
def api_client(test_lifespan: Any) -> Any:
    """A `TestClient` over the real app, with the lifespan actually run.

    The real router stack, the real auth dependencies, the real settings -- only
    the four infrastructure singletons are swapped. That is deliberate: a
    hand-built app with two routes cannot catch a dependency-wiring mistake, and
    wiring is where FastAPI projects actually break.

    Used as a context manager, which is the part that is easy to get wrong.
    `TestClient(app)` on its own does **not** run the lifespan -- `app.state` stays
    empty and the first route that reads `app.state.rate_limiter` raises
    `AttributeError: 'State' object has no attribute 'rate_limiter'`. Entering the
    `with` block is what invokes it, and `yield` from inside keeps it open for the
    test's duration.
    """
    from fastapi.testclient import TestClient

    from backend.config.settings import get_settings
    from backend.main import create_app

    app = create_app(get_settings())
    app.router.lifespan_context = test_lifespan  # type: ignore[method-assign]
    with TestClient(app) as client:
        yield client
