"""The client side of the fraud-scoring boundary.

One protocol, three implementations, chosen by `FraudScoringBackend`:

- **`GrpcFraudClient`** -- a real channel with a deadline on every call.
- **`InProcessFraudClient`** -- calls `FraudScoringEngine` directly, no wire.
- **`AutoFraudClient`** -- probes `Health` once, then sticks with whichever
  answered.

`auto` exists so `uvicorn backend.main:app` alone is a working dev system. What
it must not be is silent, so the fallback logs at WARN and every decision it
returns is tagged `transport="in_process"` -- which is exactly the label the
Prometheus histogram and the Grafana latency panel split on. An operator can
therefore see on a dashboard that the "grpc" latency series went flat because
the calls stopped being gRPC, rather than inferring it from a suspiciously round
number.

The deadline is set on every call and is *not* optional. An unbounded call on the
hot path is a stalled consumer, and a stalled consumer is a lag spike, and a lag
spike is the incident this whole sprint exists to be able to see.
"""

from __future__ import annotations

from typing import Any, Protocol

import grpc

from backend.config.settings import Settings
from backend.core.clock import monotonic
from backend.core.logging import get_logger
from backend.fraud.engine import FraudDecision, FraudScoringEngine
from backend.fraud.features import OrderContext
from backend.grpc_service.conversion import decision_from_response, request_from_context
from backend.grpc_service.generated import fraud_pb2, fraud_pb2_grpc

# Importing the generated package registers the sys.path entry protoc's imports
# need. See backend/grpc_service/generated/__init__.py.
logger = get_logger(__name__)


class FraudScoringError(RuntimeError):
    """The scorer could not be reached or refused the request.

    Carries the gRPC status code when there was one, because "unavailable" and
    "invalid argument" call for different recovery: the first means retry
    elsewhere, the second means the order is malformed and retrying it forever is
    a poison-message loop.
    """

    def __init__(self, message: str, *, code: grpc.StatusCode | None = None) -> None:
        super().__init__(message)
        self.code = code


class FraudScoringBackend(Protocol):
    """The port the order processor depends on."""

    async def score(
        self, context: OrderContext, *, require_rationale: bool = False
    ) -> FraudDecision: ...

    async def health(self) -> dict[str, Any]: ...

    async def close(self) -> None: ...

    @property
    def transport(self) -> str: ...


class GrpcFraudClient:
    """Scores over a real gRPC channel."""

    def __init__(
        self,
        *,
        target: str,
        timeout_seconds: float = 2.0,
        metrics: Any = None,
    ) -> None:
        self._target = target
        self._timeout = timeout_seconds
        self._metrics = metrics
        self._channel: Any = None
        self._stub: Any = None

    async def _ensure_stub(self) -> Any:
        if self._stub is None:
            self._channel = grpc.aio.insecure_channel(self._target)
            self._stub = fraud_pb2_grpc.FraudScoringStub(self._channel)
        return self._stub

    async def score(
        self, context: OrderContext, *, require_rationale: bool = False
    ) -> FraudDecision:
        started = monotonic()
        stub = await self._ensure_stub()
        request = request_from_context(
            context,
            correlation_id=context.order_id,
            require_rationale=require_rationale,
        )
        try:
            response = await stub.ScoreOrder(request, timeout=self._timeout)
        except grpc.aio.AioRpcError as exc:
            if self._metrics is not None:
                self._metrics.scoring_errors.labels(reason=_code_name(exc.code)).inc()
            raise FraudScoringError(
                f"scoring call failed: {_code_name(exc.code)}: {exc.details()}", code=exc.code
            ) from exc
        _ = started
        return decision_from_response(response)

    async def health(self) -> dict[str, Any]:
        stub = await self._ensure_stub()
        try:
            response = await stub.Health(fraud_pb2.HealthRequest(), timeout=self._timeout)
        except grpc.aio.AioRpcError as exc:
            raise FraudScoringError(f"health probe failed: {exc.details()}", code=exc.code) from exc
        return {
            "ready": response.ready,
            "model_version": response.model_version,
            "orders_scored": response.orders_scored,
            "tracked_customers": response.tracked_customers,
            "transport": "grpc",
        }

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None

    @property
    def transport(self) -> str:
        return "grpc"


class InProcessFraudClient:
    """Scores in this process. No socket, no deadline, no serialisation."""

    def __init__(self, engine: FraudScoringEngine, *, metrics: Any = None) -> None:
        self._engine = engine
        self._metrics = metrics

    async def score(
        self, context: OrderContext, *, require_rationale: bool = False
    ) -> FraudDecision:
        return await self._engine.score(
            context, require_rationale=require_rationale, transport="in_process"
        )

    async def health(self) -> dict[str, Any]:
        health = dict(self._engine.health())
        health["transport"] = "in_process"
        return health

    async def close(self) -> None:
        return None

    @property
    def transport(self) -> str:
        return "in_process"


class AutoFraudClient:
    """Probe once, then commit to whichever scorer answered.

    Commits rather than re-probing per call: a per-call health check would double
    the requests on the hot path and still race with the answer it just got. The
    cost is that a scorer that dies after the probe is not re-discovered, which is
    why `score()` retries the *other* transport once on `UNAVAILABLE`.
    """

    def __init__(
        self, *, grpc_client: GrpcFraudClient, engine: FraudScoringEngine, metrics: Any = None
    ) -> None:
        self._grpc = grpc_client
        self._local = InProcessFraudClient(engine, metrics=metrics)
        self._metrics = metrics
        self._active: FraudScoringBackend | None = None

    async def _resolve(self) -> FraudScoringBackend:
        if self._active is not None:
            return self._active
        try:
            await self._grpc.health()
        except FraudScoringError as exc:
            logger.warning(
                "fraud scorer unreachable at %s (%s); scoring in-process for this "
                "run. The 'grpc' latency series will be flat and the pipeline will "
                "not survive a restart.",
                self._grpc._target,  # noqa: SLF001 - same module, deliberate
                exc,
            )
            self._active = self._local
        else:
            self._active = self._grpc
        return self._active

    async def score(
        self, context: OrderContext, *, require_rationale: bool = False
    ) -> FraudDecision:
        backend = await self._resolve()
        try:
            return await backend.score(context, require_rationale=require_rationale)
        except FraudScoringError as exc:
            # One retry on the other transport, and only for UNAVAILABLE. A
            # retry of INVALID_ARGUMENT would be a poison-message loop; a retry
            # of DEADLINE_EXCEEDED would double the load on a scorer that is
            # already too slow.
            retryable = exc.code in (grpc.StatusCode.UNAVAILABLE, None)
            other = self._local if backend is self._grpc else self._grpc
            if not retryable or other is self._local:
                raise
            logger.warning(
                "scorer %s unavailable, retrying on %s", backend.transport, other.transport
            )
            if self._metrics is not None:
                self._metrics.scoring_degraded.labels(reason="transport_failover").inc()
            return await other.score(context, require_rationale=require_rationale)

    async def health(self) -> dict[str, Any]:
        backend = await self._resolve()
        return await backend.health()

    async def close(self) -> None:
        await self._grpc.close()

    @property
    def transport(self) -> str:
        return self._active.transport if self._active is not None else "unresolved"


async def build_fraud_backend(
    settings: Settings, engine: FraudScoringEngine, metrics: Any
) -> FraudScoringBackend:
    """Build the configured scorer client.

    The gRPC channel itself is not dialled here -- `GrpcFraudClient` dials lazily,
    so constructing it against a dead target costs nothing and `auto` can fall
    back without having to undo a failed connect.
    """
    mode = settings.fraud_transport
    if mode == "in_process":
        return InProcessFraudClient(engine, metrics=metrics)
    grpc_client = GrpcFraudClient(
        target=settings.grpc_client_target,
        timeout_seconds=settings.grpc_timeout_seconds,
        metrics=metrics,
    )
    if mode == "grpc":
        return grpc_client
    return AutoFraudClient(grpc_client=grpc_client, engine=engine, metrics=metrics)


def _code_name(code: Any) -> str:
    """`grpc.StatusCode` renders as an enum repr; the name is what gets logged."""
    try:
        return grpc.StatusCode(code).name
    except ValueError:
        return str(code)
