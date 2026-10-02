"""The FastAPI application: lifespan, wiring, and the composition root.

Every long-lived object -- the event bus, the task queue, the scorer, the metrics
registry, the Redis clients -- is built exactly once in the lifespan hook and
attached to `app.state`. Nothing constructs a Kafka producer or a gRPC channel
per request; see `backend/api/routes/orders.py::get_event_bus` for why that
matters at 500 orders/sec.

**The app refuses to start insecurely.** Two checks in `_assert_safe_defaults`:
auth must be enabled in production, and the JWT secret must not be the committed
development default. Both are startup failures rather than warnings, because a
security check that logs and continues has already failed -- it just told
somebody.

Consumers are optional here. The API does not need to run the order processor,
because in the real topology that is a separate process (`make orders`). Running
one in-process is a development convenience and `FLOWMESH_API_INLINE_CONSUMERS`
gates it, so nobody ships a box where the API and the consumer restart together
and a consumer bug takes the public endpoint with it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware

from backend.api.ratelimit import IdempotencyCache, InMemoryIdempotencyCache, RateLimiter
from backend.api.routes import inventory as inventory_routes
from backend.api.routes import ops as ops_routes
from backend.api.routes import orders as order_routes
from backend.api.routes import reviews as review_routes
from backend.api.schemas import LoginRequest, TokenResponse
from backend.auth.dependencies import AuthService
from backend.config.settings import Settings, get_settings
from backend.core.logging import configure_logging, get_logger
from backend.database.session import create_all, dispose_engines, get_engine, get_session_factory
from backend.events.bus import EventBus
from backend.events.factory import create_event_bus
from backend.events.schema import Topic
from backend.fraud.engine import FraudScoringEngine
from backend.grpc_service.client import FraudScoringBackend, build_fraud_backend
from backend.observability.metrics import get_metrics
from backend.queues.factory import create_task_queue
from backend.queues.protocol import TaskQueue

logger = get_logger(__name__)

DESCRIPTION = """
Order ingress and the review dashboard for Cascade Retail Group.

`POST /orders` accepts an order and publishes it for real-time fraud scoring and
inventory reservation. `GET /reviews` is the supervisor queue. `/metrics` is the
Prometheus endpoint behind the Grafana dashboards in `observability/grafana/`.
"""


def _assert_safe_defaults(settings: Settings) -> None:
    """Refuse to start in a configuration that must never reach a network."""
    insecure_default = "dev-only-insecure-secret-change-me"
    if settings.app_env.lower() == "production":
        if not settings.auth_enabled:
            raise RuntimeError(
                "FLOWMESH_AUTH_ENABLED=false is refused when FLOWMESH_APP_ENV=production"
            )
        if settings.jwt_secret == insecure_default:
            raise RuntimeError(
                "FLOWMESH_JWT_SECRET is still the development default; refusing to start"
            )
        if settings.event_transport != "kafka":
            raise RuntimeError(
                "FLOWMESH_EVENT_TRANSPORT must be 'kafka' in production: the in-process "
                "log is lost on restart"
            )
        if settings.queue_transport != "rabbitmq":
            raise RuntimeError(
                "FLOWMESH_QUEUE_TRANSPORT must be 'rabbitmq' in production: review "
                "requests must survive a restart"
            )
    elif settings.jwt_secret == insecure_default:
        logger.warning(
            "using the development JWT secret; set FLOWMESH_JWT_SECRET before this "
            "is reachable by anything but you"
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build every long-lived dependency, then tear them all down."""
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_level_json)
    _assert_safe_defaults(settings)

    metrics = get_metrics()
    engine = get_engine(settings)
    if settings.storage_kind == "sqlite":
        # Only for the SQLite path. PostgreSQL gets its schema from `make migrate`,
        # because a schema that exists because a script ran is a schema nobody can
        # diff against a migration.
        await create_all(engine)
    session_factory = get_session_factory(settings)

    bus: EventBus = await create_event_bus(settings, metrics)
    queue: TaskQueue = await create_task_queue(settings, metrics)

    scorer_engine = FraudScoringEngine(settings, metrics)
    scorer: FraudScoringBackend = await build_fraud_backend(settings, scorer_engine, metrics)

    limiter = _build_limiter(settings)
    idem_cache = _build_idempotency_cache(settings)

    app.state.settings = settings
    app.state.metrics = metrics
    app.state.event_bus = bus
    app.state.task_queue = queue
    app.state.scorer = scorer
    app.state.scorer_engine = scorer_engine
    app.state.rate_limiter = limiter
    app.state.idempotency_cache = idem_cache
    app.state.session_factory = session_factory
    app.state.auth = AuthService(settings)
    app.state.event_transport = settings.event_transport
    app.state.queue_transport = settings.queue_transport
    app.state.fraud_transport = scorer.transport
    app.state.model_version = scorer_engine.model.version
    app.state.degraded = scorer_engine.degraded

    topics = await _ensure_topics(bus, settings)
    logger.info(
        "flowmesh api ready: event_transport=%s queue_transport=%s fraud=%s topics=%s",
        settings.event_transport,
        settings.queue_transport,
        scorer.transport,
        topics,
    )
    if scorer_engine.degraded:
        logger.error(
            "API started with the FALLBACK fraud model (%s); every order will be held "
            "for review until the weights file is present",
            scorer_engine.model.version,
        )

    try:
        yield
    finally:
        await scorer.close()
        await queue.stop()
        await bus.stop()
        await limiter.close()
        await idem_cache.close()
        await dispose_engines()
        logger.info("flowmesh api stopped")


async def _ensure_topics(bus: EventBus, settings: Settings) -> list[str]:
    try:
        await bus.ensure_topics(settings.kafka_partitions)
    except Exception as exc:  # noqa: BLE001 - a broker problem must not block startup
        logger.warning("could not ensure topics at startup: %s", exc)
    return [str(Topic.ORDER), str(Topic.INVENTORY)]


def _build_limiter(settings: Settings) -> RateLimiter:
    """Redis limiter, or the in-process one for the SQLite path.

    The choice follows the database: the SQLite path is the single-process
    development and load-test setup, where a per-process counter is exactly as
    correct as a shared one and does not need a server it does not have.
    """
    if settings.storage_kind == "sqlite":
        from backend.api.ratelimit import InMemoryRateLimiter

        return InMemoryRateLimiter(limit_per_minute=settings.rate_limit_per_minute)
    return RateLimiter(url=settings.redis_url, limit_per_minute=settings.rate_limit_per_minute)


def _build_idempotency_cache(settings: Settings) -> IdempotencyCache:
    if settings.storage_kind == "sqlite":
        return InMemoryIdempotencyCache()
    return IdempotencyCache(url=settings.redis_url)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the application. The factory exists so tests can build their own."""
    resolved = settings or get_settings()
    app = FastAPI(
        title="FlowMesh API",
        description=DESCRIPTION,
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        # The dashboard is served by Grafana, which is a different origin. The
        # default `allow_origins=["*"]` is not acceptable on an endpoint that can
        # approve orders, so this lists what is actually used.
        allow_origins=[
            "http://localhost:3000",
            "http://localhost:8000",
        ],
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type", "Idempotency-Key"],
    )
    app.include_router(order_routes.router)
    app.include_router(review_routes.router)
    app.include_router(inventory_routes.router)
    app.include_router(ops_routes.router)
    app.include_router(auth_router, prefix="/auth", tags=["auth"])
    app.state.declared_settings = resolved
    return app


def get_auth(request: Request) -> AuthService:
    """The auth service, off `app.state` where the lifespan put it.

    A dependency rather than a global reach inside the handler: the version that
    did `login.app.state` would have been untestable and would have raised
    `AttributeError` rather than a 500 in production.

    **Module scope, and that is load-bearing rather than tidiness.** This module
    has `from __future__ import annotations`, so every annotation below is a
    *string* that FastAPI resolves against this module's globals. When the handler,
    its dependency and the schema names were all defined inside `_auth_router()`,
    `get_auth` was a local name and nothing in the annotations could be resolved:
    FastAPI demoted `body: LoginRequest` to a query parameter and reported `auth` as
    a missing query parameter, so the endpoint answered *every* request with 422 and
    logged in nobody -- on the exact route `README.md`'s quick start calls.

    Nothing raised and nothing failed to import; the OpenAPI schema quietly
    documented query parameters instead of a JSON body. It went unnoticed because
    no test called this endpoint: every authorisation test in the suite mints its
    own token with `create_access_token`, so the one route that turns a password
    into a token had no coverage. `test_login_returns_a_token_for_a_seeded_user`
    covers it now.

    The rule this encodes: in a PEP 563 module, a FastAPI handler, the names in its
    annotations, and anything its annotations reference -- including `Depends`
    targets -- must all be reachable as module globals.
    """
    return request.app.state.auth  # type: ignore[no-any-return]


auth_router = APIRouter()


@auth_router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    auth: Annotated[AuthService, Depends(get_auth)],
) -> TokenResponse:
    result = auth.login(body.email, body.password)
    if result is None:
        # One message for both "no such user" and "wrong password", and a 401
        # for both. Distinguishing them is account enumeration.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        )
    return TokenResponse(**result)  # type: ignore[arg-type]


app = create_app()
