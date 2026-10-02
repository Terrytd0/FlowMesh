"""The task-queue port.

The second broker in this pipeline, and the one that generates the most
interview questions, because "why Kafka *and* RabbitMQ" is not a question with
a fashionable answer -- it is a question about failure modes. ADR-004 has the
argument; the short version is:

- **Kafka is the log of what happened.** Replayable, partitioned by key,
  high-throughput, and its consumer offsets are a *position in history*.
- **RabbitMQ is the queue of what still needs doing.** Acknowledged, routed,
  dead-lettered, redelivered on nack, with a TTL on a message that nobody wants.

`review.requested` belongs to the second kind. If it is lost, a fraudulent order
sits unscored and the customer is charged; if it is replayed three hours later
it is worse than useless. RabbitMQ's ack/nack/dead-letter vocabulary maps onto
that requirement exactly, and Kafka's does not -- a "review requested" event
that has been consumed by the review worker and then failed to be recorded is,
to Kafka, *delivered successfully*.

Deliberately absent from this port: request/response, priorities per se, and
any notion of "the message is the queue". One `publish`, one `consume`, one
`ack`/`nack`, one dead-letter queue. That is the whole surface this pipeline
needs, and a port wider than the need is a port with untested branches.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from backend.core.logging import get_logger
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)

TaskHandler = Callable[[Any], Awaitable[None]]
"""Handles one task body. Raise to nack; return to ack."""


class QueueUnavailableError(RuntimeError):
    """The configured broker could not be reached."""


class DeadLetteredError(RuntimeError):
    """A task exhausted its delivery attempts and was dead-lettered."""


@runtime_checkable
class TaskQueue(Protocol):
    """The port. Two implementations: RabbitMQ and the in-process queue."""

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(
        self, queue: str, body: bytes, *, headers: dict[str, str] | None = None
    ) -> None: ...

    async def consume(self, queue: str, handler: TaskHandler, *, concurrency: int = 1) -> None: ...

    async def depth(self, queue: str) -> dict[str, int]:
        """`{"ready", "unacked", "dead_letter"}` for the queue-depth gauge."""
        ...


def record_depth(metrics: FlowMeshMetrics, queue: str, depths: dict[str, int]) -> None:
    """Publish a queue's depth to the gauge.

    Shared by both implementations so the gauge's meaning cannot drift between
    the real broker and the in-process queue.
    """
    metrics.set_queue_depth(
        queue,
        ready=int(depths.get("ready", 0)),
        unacked=int(depths.get("unacked", 0)),
        dead_letter=int(depths.get("dead_letter", 0)),
    )
