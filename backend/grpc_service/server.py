"""The fraud-scoring gRPC server.

Status-code mapping, which is the part worth reading (ADR-007). The "why" column
is the point -- the same code means something different depending on who reads
it:

| situation | code | why that code |
| --- | --- | --- |
| bad `order_id`, negative total, no items | `INVALID_ARGUMENT` | the caller repeats it; retrying cannot help |
| deadline elapsed | `DEADLINE_EXCEEDED` | retrying elsewhere is right, retrying here now is not |
| model not loaded | `UNAVAILABLE` | a replica may be healthy; `auto` probes for this |
| anything else | `INTERNAL` | genuinely unexpected, logged with a stack trace |

`UNAVAILABLE` for an unloaded model is the load-bearing choice. A cold replica
returns `UNAVAILABLE`; a `FAILED_PRECONDITION` would read to the client as "this
request is bad" and it would drop the order instead of trying elsewhere.

The server is also the only place that binds a port, and it binds `port=0` in
tests -- see `tests/integration/test_fraud_grpc_socket.py`, which is the only
test that runs this class for real.
"""

from __future__ import annotations

import asyncio
from concurrent import futures
from typing import Any

import grpc

from backend.config.settings import Settings
from backend.core.logging import configure_logging, get_logger
from backend.fraud.engine import FraudScoringEngine
from backend.grpc_service.conversion import context_from_request, response_from_decision
from backend.grpc_service.generated import fraud_pb2, fraud_pb2_grpc
from backend.observability.metrics import FlowMeshMetrics, get_metrics

# Importing the generated package registers the sys.path entry that protoc's
# `from flowmesh.v1 import ...` imports need. See that package's README.
logger = get_logger(__name__)


class FraudScoringServicer(fraud_pb2_grpc.FraudScoringServicer):
    """Adapts the transport-free engine to the generated servicer base class."""

    def __init__(self, engine: FraudScoringEngine, metrics: FlowMeshMetrics | None = None) -> None:
        self._engine = engine
        self._metrics = metrics or get_metrics()

    async def ScoreOrder(
        self, request: fraud_pb2.FraudRequest, context: grpc.aio.ServicerContext
    ) -> fraud_pb2.FraudResponse:
        """Score one order."""
        invalid = _validate(request)
        if invalid:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, invalid)

        context_obj = context_from_request(request)
        try:
            decision = await self._engine.score(
                context_obj,
                require_rationale=request.require_rationale,
                transport="grpc",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("scoring failed order_id=%s", request.order_id)
            await context.abort(
                grpc.StatusCode.INTERNAL,
                f"scoring failed: {type(exc).__name__}: {exc}",
            )
            raise AssertionError("unreachable: abort always raises") from exc
        return response_from_decision(decision)

    async def Health(
        self, request: fraud_pb2.HealthRequest, context: grpc.aio.ServicerContext
    ) -> fraud_pb2.HealthResponse:
        """Liveness plus the numbers an operator wants on a dashboard."""
        health: dict[str, Any] = self._engine.health()
        return fraud_pb2.HealthResponse(
            ready=bool(health["ready"]),
            model_version=str(health["model_version"]),
            orders_scored=int(health["orders_scored"]),
            tracked_customers=int(health["tracked_customers"]),
        )


def _validate(request: fraud_pb2.FraudRequest) -> str:
    """Return a human-readable reason the request is unacceptable, or ''."""
    if not request.order_id:
        return "order_id is required"
    if not request.customer_id:
        return "customer_id is required"
    if request.total_cents < 0:
        return f"total_cents must not be negative (got {request.total_cents})"
    if not request.items:
        return "at least one line item is required"
    for item in request.items:
        if item.quantity < 1:
            return f"item {item.sku!r} has quantity {item.quantity}, must be >= 1"
    return ""


async def serve(
    settings: Settings,
    engine: FraudScoringEngine,
    *,
    port: int | None = None,
    ready: asyncio.Event | None = None,
) -> tuple[grpc.aio.Server, int]:
    """Start the server and return it with the bound port.

    Returns the bound port rather than the configured one because tests bind
    `port=0` and need to know which port the OS chose. The alternative -- reading
    back `settings.grpc_server_port` -- is how a test ends up talking to whatever
    else is listening on 50052.
    """
    server = grpc.aio.server(
        options=[
            # Keepalive tuned for a long-lived connection across a container
            # network. The defaults (infinite) mean a silently dead peer is
            # noticed only by the per-call deadline, which turns one dead socket
            # into every scoring call failing.
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.max_concurrent_streams", 256),
        ]
    )
    fraud_pb2_grpc.add_FraudScoringServicer_to_server(
        FraudScoringServicer(engine, get_metrics()), server
    )
    bind_port = settings.grpc_server_port if port is None else port
    bound = server.add_insecure_port(f"{settings.grpc_server_host}:{bind_port}")
    if bound == 0:
        raise RuntimeError(f"cannot bind {settings.grpc_server_host}:{bind_port}")

    await server.start()
    if ready is not None:
        ready.set()
    logger.info("fraud scoring gRPC server listening on port %s", bound)
    return server, bound


async def serve_forever(settings: Settings) -> None:
    """The process entry point: configure logging, build the engine, serve."""
    configure_logging(settings.log_level, json_output=settings.log_level_json)
    metrics = get_metrics()
    engine = FraudScoringEngine(settings, metrics)
    if engine.degraded:
        logger.error(
            "scoring service started DEGRADED (model %s); every decision will be "
            "marked degraded and routed to review",
            engine.model.version,
        )
    server, _bound = await serve(settings, engine)
    await server.wait_for_termination()


def main() -> None:
    """Console entry point (`flowmesh-fraud`)."""
    from backend.config.settings import get_settings

    asyncio.run(serve_forever(get_settings()))


def build_blocking_server(settings: Settings, engine: FraudScoringEngine) -> Any:
    """A synchronous server, for tooling that wants one.

    Exists because `grpcurl`-style smoke checks and the CI container job are
    easier to write against a blocking server, and maintaining two server
    implementations would be a second place for the status mapping to drift.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    fraud_pb2_grpc.add_FraudScoringServicer_to_server(
        FraudScoringServicer(engine, get_metrics()), server
    )
    return server


__all__ = ["FraudScoringServicer", "main", "serve", "serve_forever"]
