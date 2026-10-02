"""Operational endpoints: health, readiness, and `/metrics`.

`/metrics` is the endpoint the whole observability story rests on, and it is
deliberately dependency-free: it renders the registry that `prometheus_client`
already holds and touches no database, no broker and no LLM. A `/metrics` that
can block on a failing dependency is a `/metrics` that stops answering exactly
when it is needed, and a Prometheus scrape timeout looks identical to a dead
service.

`/healthz` is liveness: the process is up. It does *not* check the database or
the brokers, because a liveness probe that fails when Postgres restarts gets the
API killed, and a killed API does not reconnect to Postgres when it comes back.

`/readyz` is readiness: can this instance actually take an order? That does check
the dependencies, and a `503` from it removes the instance from the load balancer
without killing it. The distinction is the difference between self-healing and a
restart loop.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request, Response, status
from sqlalchemy import text

from backend.api.schemas import HealthOut
from backend.auth.dependencies import CurrentPrincipal, DbSession
from backend.core.logging import get_logger
from backend.database.repositories import orders as order_repo
from backend.observability.metrics import get_metrics

logger = get_logger(__name__)

router = APIRouter(tags=["ops"])


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness. No dependencies touched -- see the module docstring."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request, session: DbSession) -> Response:
    """Readiness. Checks the database, reports transport state.

    A `503` here removes the instance from rotation; it does not kill it.
    """
    database_ok = True
    detail = "ok"
    try:
        await session.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - the point is to report, not to raise
        database_ok = False
        detail = f"{type(exc).__name__}: {exc}"[:200]
        logger.error("readiness check failed: %s", detail)

    payload: dict[str, Any] = {
        "database": "ok" if database_ok else "unavailable",
        "event_transport": request.app.state.event_transport,
        "queue_transport": request.app.state.queue_transport,
        "fraud_transport": request.app.state.fraud_transport,
    }
    if not database_ok:
        payload["detail"] = detail
    return Response(
        content=json.dumps(payload),
        status_code=status.HTTP_200_OK if database_ok else status.HTTP_503_SERVICE_UNAVAILABLE,
        media_type="application/json",
    )


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Prometheus exposition format.

    No label of any kind is derived from user input. A metric label is a
    dimension in the time-series store, and a label built from an order id is
    both unbounded (millions of series) and a way to attack the monitoring
    system. The `order_id` label that exists here carries no value -- see
    `FlowMeshMetrics`: cardinality is bounded to the fixed label sets declared in
    `backend/observability/metrics.py`.
    """
    return Response(content=get_metrics().render(), media_type="text/plain; version=0.0.4")


@router.get("/health", response_model=HealthOut, tags=["ops"])
async def health(session: DbSession, principal: CurrentPrincipal, request: Request) -> HealthOut:
    """Human-readable health, for a dashboard's status widget and for the smoke script."""
    _ = principal
    database_ok = True
    try:
        await session.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001
        database_ok = False
    return HealthOut(
        status="ok" if database_ok else "degraded",
        database="ok" if database_ok else "unavailable",
        event_transport=str(request.app.state.event_transport),
        queue_transport=str(request.app.state.queue_transport),
        fraud_transport=str(request.app.state.fraud_transport),
        model_version=str(request.app.state.model_version),
        orders=await order_repo.count_orders(session),
        degraded=bool(request.app.state.degraded),
    )
