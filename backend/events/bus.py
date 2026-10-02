"""The event-log port: what the pipeline is allowed to assume about a broker.

`EventBus` is the interface. There are two implementations -- `KafkaEventBus`
and `InMemoryEventLog` -- and the rest of the pipeline is written against the
port, never against either. That is not for test convenience; it is the reason
the unit suite can assert on *offsets, redelivery and group isolation* rather
than on mock call lists, because the in-process implementation is a real
partitioned log with real committed offsets (see ADR-005).

Guares the port makes explicit, because every one of them is a place where a
naive implementation silently does the wrong thing:

- **Delivery is at-least-once.** A handler may see the same envelope twice, and
  a handler that is not idempotent will corrupt state. Every handler in
  `backend/pipeline/` records `event_id` in `processed_events` inside the same
  transaction as its effect. Do not "optimise" that away.
- **Per-key ordering, not global ordering.** Records with the same key land on
  one partition and are delivered in order; records with different keys may
  interleave. Any assumption of a total order across orders is wrong.
- **The offset is committed only after the handler returns.** A crash mid-
  handler replays that record. This is what makes the chaos test's "zero data
  loss" claim testable rather than aspirational.
- **Publishing returns the assigned offset.** Producers use it to build
  causation chains without a second round trip.
"""

from __future__ import annotations

import asyncio
import zlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from backend.core.logging import get_logger
from backend.events.schema import EventEnvelope, Topic
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)

Handler = Callable[[EventEnvelope], Awaitable[None]]
"""A handler. Returns when the effect is durably applied -- not when it started."""


class EventBusUnavailableError(RuntimeError):
    """The configured broker could not be reached.

    Distinct from a handler failing. This one means "no events are flowing", and
    it is what makes the `auto` transport fall back rather than hang.
    """


class HandlerFailedError(RuntimeError):
    """A handler raised and exhausted its retries.

    The subscription is stopped rather than skipped past, deliberately: Kafka's
    answer to "what if a record cannot be processed" is *stop consuming*, because
    committing past it is silent data loss. Recovery is operational (fix the
    bug, restart the consumer) and the offset makes the replay exact.
    """

    def __init__(self, *, topic: str, group: str, partition: int, offset: int, cause: Exception):
        super().__init__(
            f"handler failed at {topic}[{partition}]@{offset} group={group}: "
            f"{type(cause).__name__}: {cause}"
        )
        self.topic = topic
        self.group = group
        self.partition = partition
        self.offset = offset
        self.cause = cause


@dataclass(frozen=True)
class Subscription:
    """One consumer's interest in one topic.

    `instances`/`instance_id` model a consumer *group* spread over several
    processes: partition `p` belongs to instance `p % instances`. This exists so
    the "scale consumers on partition lag" stretch goal is a real assignment
    rule that can be asserted on, rather than a sentence in a README.
    """

    topic: Topic
    group: str
    handler: Handler
    instances: int = 1
    instance_id: int = 0

    def __post_init__(self) -> None:
        if self.instances < 1:
            raise ValueError(f"instances must be >= 1, got {self.instances}")
        if not 0 <= self.instance_id < self.instances:
            raise ValueError(
                f"instance_id must be in [0, {self.instances}), got {self.instance_id}"
            )


@dataclass(eq=False)
class ConsumerHandle:
    """A running subscription, and the knobs that control it.

    `eq=False` so the dataclass keeps its identity hash: the buses keep a `set` of
    live handles so `stop()` can cancel them all, and an `eq=True` dataclass with
    a mutable `task` field would try to hash that field -- which is either an
    unhashable Task or, worse, two distinct subscriptions comparing equal because
    their tasks happened to match.
    """

    subscription: Subscription
    assigned_partitions: list[int] = field(default_factory=list)
    #: Set by the implementation; `bus.cancel(handle)` stops the loop cleanly.
    task: asyncio.Task[None] | None = None
    stopped: bool = False

    @property
    def group(self) -> str:
        return self.subscription.group

    @property
    def topic(self) -> Topic:
        return self.subscription.topic


@runtime_checkable
class EventBus(Protocol):
    """The port. See the module docstring for the guarantees it carries."""

    async def start(self) -> None:
        """Connect. Idempotent."""
        ...

    async def stop(self) -> None:
        """Disconnect and drop every subscription. Idempotent."""
        ...

    async def publish(self, topic: Topic, key: str, envelope: EventEnvelope) -> int:
        """Append one record. Returns its offset within its partition."""
        ...

    async def ensure_topics(self, partitions: int) -> None:
        """Create the topics if absent. No-op when they exist."""
        ...

    async def subscribe(self, subscription: Subscription) -> ConsumerHandle:
        """Begin consuming from each assigned partition's committed offset."""
        ...

    async def cancel(self, handle: ConsumerHandle) -> None:
        """Stop one subscription without touching its committed offsets."""
        ...

    async def committed_offsets(self, topic: Topic, group: str) -> dict[int, int]:
        """Committed offset per partition -- what a restart would resume from."""
        ...

    async def end_offsets(self, topic: Topic) -> dict[int, int]:
        """High-water mark per partition, i.e. how many records exist."""
        ...

    async def lag(self, topic: Topic, group: str) -> int:
        """Total records across partitions that this group has not committed."""
        ...

    async def replay(self, topic: Topic, from_offset: int = 0) -> list[EventEnvelope]:
        """Every record from `from_offset`, ignoring group offsets.

        The reconciliation primitive. "Zero data loss after a consumer is killed"
        is only a claim until something can read the log back and compare it to
        the database, and this is that something.
        """
        ...


def partition_for(key: str, partitions: int) -> int:
    """Kafka's default partitioning: CRC32 of the key, modulo the partition count.

    crc32 rather than `hash()` because Python's string hash is randomised per
    process (PYTHONHASHSEED). Two services choosing different partitions for
    the same key would put one order's events on two different consumers and
    destroy per-key ordering the moment the fleet scaled past one process.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be >= 1, got {partitions}")
    return zlib.crc32(key.encode("utf-8")) % partitions


def assign_partitions(partitions: int, instances: int, instance_id: int) -> list[int]:
    """Round-robin assignment of partitions to consumer instances."""
    if instances < 1:
        raise ValueError(f"instances must be >= 1, got {instances}")
    if not 0 <= instance_id < instances:
        raise ValueError(f"instance_id must be in [0, {instances}), got {instance_id}")
    return [index for index in range(partitions) if index % instances == instance_id]


async def dispatch_with_retries(
    *,
    handler: Handler,
    envelope: EventEnvelope,
    metrics: FlowMeshMetrics,
    topic: str,
    group: str,
    max_attempts: int = 3,
    backoff_seconds: float = 0.05,
) -> None:
    """Run one handler with bounded retries, counting every redelivery.

    The retry lives here rather than in each bus because the two implementations
    must agree: if the in-process log retried and Kafka did not, the chaos test
    would pass locally and lose orders in production.

    Raises `HandlerFailedError` once the attempts are exhausted. It does *not*
    swallow: committing past a record that cannot be processed is how an event
    pipeline loses data quietly, and the caller stops the subscription instead.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            await handler(envelope)
            return
        except Exception as exc:
            if attempt >= max_attempts:
                metrics.events_consumed.labels(topic=topic, group=group, outcome="error").inc()
                raise HandlerFailedError(
                    topic=topic,
                    group=group,
                    partition=envelope.partition or 0,
                    offset=envelope.offset or 0,
                    cause=exc,
                ) from exc
            metrics.events_redelivered.labels(topic=topic, group=group).inc()
            logger.warning(
                "handler failed, redelivering: event_id=%s event_type=%s attempt=%s error=%s",
                envelope.event_id,
                str(envelope.event_type),
                attempt,
                f"{type(exc).__name__}: {exc}",
            )
            import asyncio

            await asyncio.sleep(backoff_seconds * attempt)
