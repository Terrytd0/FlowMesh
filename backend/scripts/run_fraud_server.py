"""Run the fraud scoring gRPC service.

See `backend/grpc_service/server.py` for the status-code mapping, which is the
part worth reading before changing anything here.
"""

from __future__ import annotations

import asyncio

from backend.config.settings import get_settings
from backend.core.logging import configure_logging, get_logger
from backend.fraud.engine import FraudScoringEngine
from backend.observability.metrics import get_metrics
from backend.observability.serve import serve_metrics

logger = get_logger(__name__)


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_level_json)

    from backend.grpc_service.server import serve

    metrics = get_metrics()
    engine = FraudScoringEngine(settings, metrics)
    if engine.degraded:
        # Loud, because a degraded scorer changes what the pipeline does: every
        # order is held for review, and the review queue will start filling. An
        # operator who does not know why is watching a queue grow with no cause.
        logger.error(
            "scoring service started DEGRADED: the weights at %s could not be "
            "loaded, so every decision uses the conservative fallback and every "
            "order is routed to review",
            settings.fraud_model_path,
        )
    else:
        logger.info(
            "scoring service starting: model=%s low=%.2f high=%.2f llm=%s",
            engine.model.version,
            settings.fraud_low_threshold,
            settings.fraud_high_threshold,
            settings.llm_enabled,
        )

    # The gRPC service also exposes /metrics. Two protocols on one process is not
    # elegance, it is that Prometheus speaks HTTP and the scorer speaks gRPC, and
    # the compose file scrapes both from the same container.
    #
    # `fraud_metrics_port`, not `metrics_port`: the latter defaults to 50052, the
    # same number as `grpc_server_port`, and the gRPC bind then fails with "address
    # already in use" pointing at the wrong culprit. `Settings._scorer_ports_differ`
    # rejects the collision at load time rather than letting it reach a deploy.
    metrics_server = await serve_metrics(metrics, port=settings.fraud_metrics_port)
    server, bound = await serve(settings, engine)
    try:
        await server.wait_for_termination()
    finally:
        logger.info(
            "scoring service stopped: orders_scored=%s customers=%s",
            engine.orders_scored,
            engine.tracked_customers(),
        )
        metrics_server.close()
        await metrics_server.wait_closed()
        _ = bound


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
