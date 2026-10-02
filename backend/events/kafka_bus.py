"""The Kafka implementation of the event-log port.

Deliberately close to `InMemoryEventLog` in behaviour, because the two are
interchangeable by contract and every guarantee in `backend/events/bus.py` has
to hold for both. Where Kafka genuinely differs, the difference is documented
here rather than papered over:

- **Offset commits are asynchronous.** `commit()` returns before the broker has
  persisted the offset, so a crash can replay a record the handler already
  applied. That is why every handler is idempotent on `event_id`; it is not
  belt-and-braces, it is the required behaviour of at-least-once delivery.
- **Assignment is the group coordinator's job.** This client does not
  implement it; `Subscription.instances` is used only to name the consumer, and
  the broker decides. (The in-process log approximates it with round-robin,
  documented in ADR-005 as a difference the tests must not depend on.)
- **Poll, not push.** Records arrive in batches with a timeout, so a handler
  sees `envelope.offset` values that may skip. Nothing downstream may assume
  consecutive offsets.
- **`acks="all"` plus `enable_idempotence=True`.** A publisher that loses an
  ack and retries must not produce a duplicate *physical* record; the broker
  dedupes by producer id and sequence. Deduplication across *producers* is not
  something Kafka promises, which is why `processed_events` exists anyway.
"""

from __future__ import annotations

import asyncio
from typing import Any

from backend.core.logging import get_logger
from backend.events.bus import (
    ConsumerHandle,
    EventBusUnavailableError,
    Subscription,
    dispatch_with_retries,
)
from backend.events.schema import EventEnvelope, Topic
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)

_POLL_TIMEOUT_MS = 250
_COMMIT_TIMEOUT_SECONDS = 10.0
#: How long `replay()` waits for a batch before deciding the topic is drained.
_REPLAY_IDLE_MS = 1000


class KafkaEventBus:
    """An `EventBus` backed by a real broker."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        metrics: FlowMeshMetrics,
        partitions: int = 6,
        client_id: str = "flowmesh",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._metrics = metrics
        self._partitions = partitions
        self._client_id = client_id
        self._producer: Any = None
        self._started = False
        self._consumers: dict[int, Any] = {}
        self._handles: set[ConsumerHandle] = set()

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError as exc:  # pragma: no cover - dependency is declared
            raise EventBusUnavailableError(f"aiokafka is not installed: {exc}") from exc

        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                client_id=self._client_id,
                acks="all",
                enable_idempotence=True,
                linger_ms=5,
                max_batch_size=64 * 1024,
            )
            await producer.start()
        except Exception as exc:
            raise EventBusUnavailableError(
                f"cannot reach kafka at {self._bootstrap_servers}: {type(exc).__name__}: {exc}"
            ) from exc
        self._producer = producer
        self._started = True
        logger.info("kafka producer connected: %s", self._bootstrap_servers)

    async def stop(self) -> None:
        for consumer in list(self._consumers.values()):
            await consumer.stop()
        self._consumers.clear()
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        self._started = False

    async def ensure_topics(self, partitions: int | None = None) -> None:
        """Create the topics if they do not exist. No-op when they do.

        Explicit topic creation rather than relying on auto-create because a
        broker with auto-creation disabled (the default in a serious
        deployment) fails at the *first publish* otherwise, which is the worst
        possible moment to discover it.
        """
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        from aiokafka.errors import TopicAlreadyExistsError

        count = partitions or self._partitions
        admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        try:
            await admin.start()
            try:
                await admin.create_topics(
                    [
                        NewTopic(name=str(Topic.ORDER), num_partitions=count, replication_factor=1),
                        NewTopic(
                            name=str(Topic.INVENTORY), num_partitions=count, replication_factor=1
                        ),
                    ]
                )
                logger.info("created kafka topics with %s partitions each", count)
            except TopicAlreadyExistsError:
                logger.debug("kafka topics already exist")
        except Exception as exc:
            raise EventBusUnavailableError(
                f"cannot administer kafka at {self._bootstrap_servers}: {type(exc).__name__}: {exc}"
            ) from exc
        finally:
            await admin.close()

    # -- producing ---------------------------------------------------------

    async def publish(self, topic: Topic, key: str, envelope: EventEnvelope) -> int:
        if self._producer is None:
            raise EventBusUnavailableError("kafka producer is not started")
        try:
            metadata = await self._producer.send_and_wait(
                str(topic), envelope.to_wire(), key=key.encode("utf-8")
            )
        except Exception as exc:
            raise EventBusUnavailableError(
                f"publish to {topic} failed: {type(exc).__name__}: {exc}"
            ) from exc
        self._metrics.events_published.labels(topic=str(topic)).inc()
        return int(metadata.offset)

    # -- consuming ---------------------------------------------------------

    async def subscribe(self, subscription: Subscription) -> ConsumerHandle:
        from aiokafka import AIOKafkaConsumer

        instance = subscription.instance_id
        if instance in self._consumers:
            raise EventBusUnavailableError(
                f"instance {instance} is already consuming in this process"
            )

        consumer = AIOKafkaConsumer(
            str(subscription.topic),
            bootstrap_servers=self._bootstrap_servers,
            group_id=subscription.group,
            client_id=f"{self._client_id}-{subscription.group}-{instance}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=None,
        )
        try:
            await consumer.start()
        except Exception as exc:
            raise EventBusUnavailableError(
                f"cannot join group {subscription.group}: {type(exc).__name__}: {exc}"
            ) from exc

        self._consumers[instance] = consumer
        handle = ConsumerHandle(subscription=subscription, assigned_partitions=[])
        handle.task = asyncio.create_task(
            self._run(handle, consumer), name=f"kafka:{subscription.group}:{instance}"
        )
        logger.info(
            "kafka subscription started topic=%s group=%s instance=%s/%s",
            str(subscription.topic),
            subscription.group,
            instance,
            subscription.instances,
        )
        return handle

    async def cancel(self, handle: ConsumerHandle) -> None:
        handle.stopped = True
        task = handle.task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        instance = handle.subscription.instance_id
        consumer = self._consumers.pop(instance, None)
        if consumer is not None:
            await consumer.stop()
        self._handles.discard(handle)

    async def _run(self, handle: ConsumerHandle, consumer: Any) -> None:
        from aiokafka.structs import OffsetAndMetadata

        subscription = handle.subscription
        self._handles.add(handle)
        topic_key = str(subscription.topic)
        try:
            # `getmany` rather than `async for`: it polls with a timeout, so the
            # loop can notice `handle.stopped` and shut down cleanly instead of
            # blocking on an iterator forever.
            while not handle.stopped:
                batches = await consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS)
                for topic_partition, messages in batches.items():
                    for message in messages:
                        envelope = EventEnvelope.from_wire(message.value)
                        envelope = envelope.model_copy(
                            update={
                                "offset": message.offset,
                                "partition": topic_partition.partition,
                            }
                        )
                        await dispatch_with_retries(
                            handler=subscription.handler,
                            envelope=envelope,
                            metrics=self._metrics,
                            topic=topic_key,
                            group=subscription.group,
                        )
                        if handle.stopped:
                            break
                    if handle.stopped:
                        break
                    # Commit only what the handler actually finished. Offsets
                    # are monotonic per partition, and every message in this
                    # batch was handled, so the highest one plus one is right.
                    await consumer.commit(
                        {
                            topic_partition: OffsetAndMetadata(
                                messages[-1].offset + 1, messages[-1].timestamp or ""
                            )
                        },
                        timeout_ms=int(_COMMIT_TIMEOUT_SECONDS * 1000),
                    )
                    self._metrics.events_consumed.labels(
                        topic=topic_key, group=subscription.group, outcome="approved"
                    ).inc()
                    self._report_lag(consumer, topic_key, subscription.group)
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        finally:
            self._handles.discard(handle)

    def _report_lag(self, consumer: Any, topic: str, group: str) -> None:
        """Publish total lag for this consumer's assigned partitions.

        Diagnostics only, so every failure is swallowed -- lag reporting must not
        be able to stop the consume loop it is describing.
        """
        try:
            total = 0
            for topic_partition in consumer.assignment():
                highwater = consumer.highwater(topic_partition)
                position = consumer.position(topic_partition)
                total += max(0, (highwater or 0) - (position or 0))
        except Exception:  # noqa: BLE001
            return
        self._metrics.consumer_lag.labels(topic=topic, group=group).set(total)

    # -- inspection --------------------------------------------------------

    async def committed_offsets(self, topic: Topic, group: str) -> dict[int, int]:
        from aiokafka import AIOKafkaConsumer, TopicPartition

        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=group,
            enable_auto_commit=False,
        )
        try:
            await consumer.start()
            partitions = await consumer.partitions_for_topic(str(topic))
            if not partitions:
                return {}
            topic_partitions = [TopicPartition(str(topic), index) for index in sorted(partitions)]
            committed = await consumer.committed(topic_partitions)
            return {
                tp.partition: offset
                for tp, offset in committed.items()
                if offset is not None and offset >= 0
            }
        finally:
            await consumer.stop()

    async def end_offsets(self, topic: Topic) -> dict[int, int]:
        from aiokafka import AIOKafkaConsumer, TopicPartition

        consumer = AIOKafkaConsumer(bootstrap_servers=self._bootstrap_servers)
        try:
            await consumer.start()
            partitions = await consumer.partitions_for_topic(str(topic))
            if not partitions:
                return {}
            ends = await consumer.end_offsets(
                [TopicPartition(str(topic), index) for index in sorted(partitions)]
            )
            return {tp.partition: offset for tp, offset in ends.items()}
        finally:
            await consumer.stop()

    async def lag(self, topic: Topic, group: str) -> int:
        committed = await self.committed_offsets(topic, group)
        ends = await self.end_offsets(topic)
        return sum(max(0, end - committed.get(partition, 0)) for partition, end in ends.items())

    async def replay(self, topic: Topic, from_offset: int = 0) -> list[EventEnvelope]:
        """Read the topic back from `from_offset`, ignoring group state.

        Uses `group_id=None` and an explicit `assign`, because a consumer that
        inherited a group's offsets would silently skip the very records being
        reconciled -- the one way this method could return a plausible-looking
        but wrong answer.
        """
        from aiokafka import AIOKafkaConsumer, TopicPartition

        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            partitions = await consumer.partitions_for_topic(str(topic))
            if not partitions:
                return []
            consumer.assign(
                {TopicPartition(str(topic), index): from_offset for index in sorted(partitions)}
            )
            records: list[EventEnvelope] = []
            while True:
                batches = await consumer.getmany(timeout_ms=_REPLAY_IDLE_MS)
                if not batches:
                    break
                for topic_partition, messages in batches.items():
                    for message in messages:
                        envelope = EventEnvelope.from_wire(message.value)
                        records.append(
                            envelope.model_copy(
                                update={
                                    "offset": message.offset,
                                    "partition": topic_partition.partition,
                                }
                            )
                        )
            return records
        finally:
            await consumer.stop()
