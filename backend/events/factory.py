"""Choosing an event-log implementation.

One function, called by every entry point, so "which bus is this process using"
is answerable by reading one `if`. The three modes:

| mode | behaviour |
| --- | --- |
| `kafka` | always dial the broker; fail loudly if it is not there |
| `memory` | the in-process partitioned log (ADR-005) |
| `auto` | try Kafka, fall back to the log, and **say so in the log line** |

`auto` is the default because a single-process `uvicorn backend.main:app` should
be a working system on a laptop with nothing else running. What it must never
be is *silent*: a deployment that believed it was on Kafka, silently fell back to
memory, and lost its event log on restart would be a catastrophe discovered by a
customer. So the fallback emits a WARN, and `metrics_enabled` records which
transport answered, which is what the Grafana panel reads.
"""

from __future__ import annotations

from backend.config.settings import Settings
from backend.core.logging import get_logger
from backend.events.bus import EventBus, EventBusUnavailableError
from backend.events.kafka_bus import KafkaEventBus
from backend.events.memory_log import InMemoryEventLog
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)


async def create_event_bus(settings: Settings, metrics: FlowMeshMetrics) -> EventBus:
    """Build and start the configured bus."""
    mode = settings.event_transport
    if mode == "memory":
        log = InMemoryEventLog(metrics=metrics, partitions=settings.kafka_partitions)
        await log.start()
        logger.warning(
            "event transport=memory: events live in this process only and are "
            "lost on exit. Set FLOWMESH_EVENT_TRANSPORT=kafka for a real log."
        )
        return log

    kafka = KafkaEventBus(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        metrics=metrics,
        partitions=settings.kafka_partitions,
        client_id="flowmesh",
    )
    try:
        await kafka.start()
    except EventBusUnavailableError:
        if mode == "kafka":
            raise
        logger.warning(
            "event transport=auto: kafka at %s unreachable, falling back to the "
            "in-process log. This is fine for a laptop and wrong for a deployment.",
            settings.kafka_bootstrap_servers,
        )
        log = InMemoryEventLog(metrics=metrics, partitions=settings.kafka_partitions)
        await log.start()
        return log
    await kafka.ensure_topics(settings.kafka_partitions)
    return kafka


def is_in_memory(bus: EventBus) -> bool:
    """True when `bus` is the in-process log.

    Used where a capability genuinely differs rather than for convenience: the
    load test needs `wait_for_drain` and `published()`, and pretending a Kafka
    broker can be inspected from inside the producer would be a lie.
    """
    return isinstance(bus, InMemoryEventLog)
