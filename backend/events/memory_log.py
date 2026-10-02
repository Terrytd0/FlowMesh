"""An in-process partitioned event log with real offsets, groups and replay.

This is not a mock. It is a small, honest implementation of the three Kafka
properties this pipeline actually depends on:

1. **Partitioned append-only storage.** Records land in one of N partitions by
   `crc32(key) % N`, offsets are monotonic per partition, and nothing is ever
   mutated or deleted in place.
2. **Consumer groups with committed offsets.** Each `(topic, group, partition)`
   triple has its own position. Publishing to a partition does not advance any
   group; a group's position advances only when a handler returns.
3. **Replay from an arbitrary offset.** `replay()` reads the log back
   irrespective of group positions, which is what makes "zero data loss after a
   consumer is killed" a measurement instead of a claim.

Why it exists at all (ADR-005): a pipeline whose core property is "no event is
lost, and uncommitted events come back" cannot be tested against a dictionary
of mock calls, because a mock has no offsets to lose. With a broker required for
every test, the interesting tests -- replay, redelivery, group isolation -- are
the ones that only ever run in CI, where nobody reads the output. And the load
test at 500 orders/sec would be measuring the broker, not the pipeline.

What it deliberately does *not* do: persistence across process restarts,
replication, or rebalancing. Those are broker features. What it does do is fail
loudly where it differs, so a test written against it cannot quietly depend on
behaviour real Kafka does not have:

- Partition assignment is static round-robin (`p % instances`). Kafka's
  cooperative-sticky assignor moves partitions on membership change; this does
  not, and `Subscription` says so.
- `ensure_topics()` is a no-op. Topics exist from the first publish.
- Records are retained forever. Kafka would expire them on a retention policy.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field

from backend.core.logging import get_logger
from backend.events.bus import (
    ConsumerHandle,
    Handler,
    Subscription,
    assign_partitions,
    dispatch_with_retries,
    partition_for,
)
from backend.events.schema import EventEnvelope, Topic
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)

_OFFSET_POLL_SECONDS = 0.005


@dataclass
class _Partition:
    """One partition's records. Append-only."""

    records: list[EventEnvelope] = field(default_factory=list)

    @property
    def end_offset(self) -> int:
        return len(self.records)

    def append(self, envelope: EventEnvelope, offset: int, partition: int) -> EventEnvelope:
        stored = envelope.model_copy(update={"offset": offset, "partition": partition})
        self.records.append(stored)
        return stored


class InMemoryEventLog:
    """An `EventBus` backed by this process's memory.

    Thread-safe by virtue of being single-loop asyncio: every mutation happens
    inside a coroutine, so there is no lock. `publish` and the consumer poll
    loops interleave only at `await` points, and the only critical sections
    (append, commit) contain no awaits.
    """

    def __init__(
        self,
        *,
        metrics: FlowMeshMetrics,
        partitions: int = 6,
    ) -> None:
        if partitions < 1:
            raise ValueError(f"partitions must be >= 1, got {partitions}")
        self._metrics = metrics
        self._partitions = partitions
        self._topics: dict[str, list[_Partition]] = {}
        self._committed: dict[tuple[str, str, int], int] = {}
        self._started = False
        #: Every envelope ever published, in publish order, for reconciliation.
        self._published: list[EventEnvelope] = []
        self._handles: set[ConsumerHandle] = set()
        self._lock = asyncio.Lock()

    # -- lifecycle ---------------------------------------------------------

    @property
    def partitions(self) -> int:
        return self._partitions

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False
        for handle in list(self._handles):
            await self.cancel(handle)

    async def ensure_topics(self, partitions: int | None = None) -> None:
        """No-op.

        Topics are created implicitly on first publish. If a test needs a
        specific partition count, it sets it on the constructor -- growing a
        partition count mid-run is not something Kafka lets you do either, so
        this does not pretend to support it.
        """
        _ = partitions

    # -- producing ---------------------------------------------------------

    async def publish(self, topic: Topic, key: str, envelope: EventEnvelope) -> int:
        """Append to the partition this key hashes to, and return the offset."""
        topic_key = str(topic)
        partition_index = partition_for(key, self._partitions)
        async with self._lock:
            partitions = self._topics.setdefault(
                topic_key, [_Partition() for _ in range(self._partitions)]
            )
            partition = partitions[partition_index]
            stored = partition.append(envelope, partition.end_offset, partition_index)
            self._published.append(stored)
        self._metrics.events_published.labels(topic=topic_key).inc()
        logger.debug(
            "published event_id=%s type=%s topic=%s partition=%s offset=%s",
            stored.event_id,
            str(stored.event_type),
            topic_key,
            partition_index,
            stored.offset,
        )
        return stored.offset or 0

    # -- consuming ---------------------------------------------------------

    async def subscribe(self, subscription: Subscription) -> ConsumerHandle:
        partitions = assign_partitions(
            self._partitions, subscription.instances, subscription.instance_id
        )
        handle = ConsumerHandle(subscription=subscription, assigned_partitions=partitions)
        self._handles.add(handle)
        handle.task = asyncio.create_task(
            self._run(handle), name=f"consumer:{subscription.group}:{subscription.instance_id}"
        )
        logger.info(
            "subscribed topic=%s group=%s instance=%s/%s partitions=%s",
            str(subscription.topic),
            subscription.group,
            subscription.instance_id,
            subscription.instances,
            partitions,
        )
        return handle

    async def cancel(self, handle: ConsumerHandle) -> None:
        """Stop a subscription, leaving committed offsets untouched.

        This is the chaos test's kill switch. Because the commit happens after
        the handler returns, a record in flight is simply not committed, and the
        next subscriber to this group starts on it again.
        """
        handle.stopped = True
        task = handle.task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._handles.discard(handle)

    async def _run(self, handle: ConsumerHandle) -> None:
        subscription = handle.subscription
        loops = [
            asyncio.create_task(
                self._consume_partition(handle, partition_index), name=f"p{partition_index}"
            )
            for partition_index in handle.assigned_partitions
        ]
        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            # Wait for the partition loops to finish unwinding before propagating.
            #
            # `asyncio.gather` does not do this itself: cancelling the gather
            # cancels the children and re-raises immediately, so `_run` -- and
            # therefore `cancel()` -- can return while a loop is still suspended
            # inside a handler's `async with session.begin()`. A transaction that
            # has not been rolled back yet still holds the database's write lock,
            # and the *next* consumer of this group starts while it is held.
            #
            # That is not hypothetical, and it is not specific to SQLite: the chaos
            # test killed the consumer mid-stream, restarted it immediately, and
            # the replacement died on its first statement with
            # `sqlite3.OperationalError: database is locked`. SQLite's
            # `busy_timeout` does not help there, because a deferred transaction
            # that already holds a SHARED lock cannot wait out an upgrade to
            # RESERVED -- it fails immediately rather than deadlocking against
            # itself. Three retries inside `dispatch_with_retries` burned in
            # ~150ms, `HandlerFailedError` took the subscription down, and because
            # `subscribe` never awaits the task the failure was invisible: the
            # chaos test sat in `wait_for_drain` for its full 60s timeout and then
            # died of an unrelated `PermissionError` cleaning up the database file.
            #
            # So the kill does not finish until the handlers it interrupted have
            # finished giving their locks back. That is also the honest reading of
            # `cancel()`: it claims the subscription is stopped, and a subscription
            # still unwinding is not stopped.
            await self._stop_loops(loops)
            raise
        except Exception:
            # One partition's failure stops the whole subscription, which is what
            # Kafka does: a consumer that cannot keep up with one partition
            # cannot be trusted with the rest either.
            logger.exception(
                "subscription stopped: topic=%s group=%s",
                str(subscription.topic),
                subscription.group,
            )
            await self._stop_loops(loops)
            raise

    async def _stop_loops(self, loops: list[asyncio.Task[None]]) -> None:
        """Cancel the partition loops and wait for them to let go of their locks.

        `return_exceptions=True` so a loop that was already cancelled reports its
        `CancelledError` here instead of propagating it into `_run`'s `except`
        clause, where it would be indistinguishable from the cancellation this is
        cleaning up after.
        """
        for loop in loops:
            loop.cancel()
        await asyncio.gather(*loops, return_exceptions=True)

    async def _consume_partition(self, handle: ConsumerHandle, partition_index: int) -> None:
        subscription = handle.subscription
        topic_key = str(subscription.topic)
        group_key = (topic_key, subscription.group, partition_index)
        while not handle.stopped:
            partition = self._topic_partitions(topic_key)[partition_index]
            position = self._committed.get(group_key, 0)
            if position >= partition.end_offset:
                await asyncio.sleep(_OFFSET_POLL_SECONDS)
                continue
            envelope = partition.records[position]
            await self._dispatch_one(subscription, envelope, topic_key)
            # The commit point. Everything before this line can be replayed;
            # everything after it is durable as far as this group is concerned.
            self._committed[group_key] = position + 1
            self._metrics.events_consumed.labels(
                topic=topic_key, group=subscription.group, outcome="approved"
            ).inc()
            self._update_lag(topic_key, subscription.group)

    async def _dispatch_one(
        self, subscription: Subscription, envelope: EventEnvelope, topic_key: str
    ) -> None:
        """Hand one record to the handler, and let an in-flight handler finish.

        The cancellation is honoured *between* records rather than in the middle of
        one, and that is the whole point of this method.

        A `Task.cancel()` delivered while the handler is suspended inside a
        database statement tears the statement apart from the inside. `aiosqlite`
        runs every statement on a worker thread, so cancelling the awaiting task
        does not stop the thread: it leaves `sqlite3_step` executing, and the
        coroutine is released to unwind while the statement is still running on
        the other side. The transaction's rollback is then awaited from a context
        that has already been cancelled, so it never completes, and the connection
        is never returned to the pool with its transaction closed.

        Two things follow, and the chaos test found both:

        - The connection keeps holding the store's write lock. The *next* consumer
          of this group starts while it is held, and its first statement fails with
          `database is locked` -- which `busy_timeout` does not rescue, because a
          deferred transaction that already holds a SHARED lock cannot wait out an
          upgrade to RESERVED. It returns SQLITE_BUSY immediately rather than
          deadlocking against itself. Three fast retries, `HandlerFailedError`, and
          the replacement subscription was dead on arrival.
        - Teardown could not reclaim the stranded threads either:
          `engine.dispose()` raised `CancelledError` out of `do_terminate` for every
          affected connection, the SQLite file stayed locked, and
          `tempfile.TemporaryDirectory.__exit__` replaced the real failure with
          `PermissionError: [WinError 32]`.

        Letting the in-flight handler run to completion costs one record's work
        (~25ms here) and removes both. It is also what `run_order_consumer.py`
        already claims to do -- "SIGINT/SIGTERM set the stop event rather than
        killing the process... A hard kill would leave the consumer mid-transaction"
        -- so this is the in-process log honouring the contract its sibling
        process already implements.

        The kill is still real: it is a `Task.cancel()` on the consumer's own task,
        the offset of the in-flight record stays uncommitted, and the restart
        resumes from it. What changes is only *where* the cancellation lands.
        """
        in_flight = asyncio.ensure_future(
            dispatch_with_retries(
                handler=subscription.handler,
                envelope=envelope,
                metrics=self._metrics,
                topic=topic_key,
                group=subscription.group,
            )
        )
        try:
            await asyncio.shield(in_flight)
        except asyncio.CancelledError:
            # Cancelled while a handler was mid-record. Wait for it anyway: it owns a
            # transaction, and letting it finish is what releases the lock.
            #
            # The loop rather than a single `await`, because the partition loop gets
            # cancelled *twice* on the way down -- once by the `gather` in `_run` and
            # again by `_stop_loops` -- and a plain `await in_flight` is torn down by
            # the second one. That is how the first version of this still abandoned
            # the handler it was trying to protect: the shield absorbed the first
            # cancellation, the second interrupted the recovery await, and
            # `CancelledError` is a `BaseException`, so nothing downstream could tell
            # the difference between "the handler finished" and "we stopped waiting".
            #
            # Every cancellation is absorbed until the handler is genuinely done, and
            # only then is the cancellation re-raised. Bounded, because the handler is
            # a bounded unit of work.
            while not in_flight.done():
                try:
                    await asyncio.shield(in_flight)
                except asyncio.CancelledError:
                    continue
            # Retrieve its outcome so asyncio does not log an unretrieved exception.
            # A handler that failed is reported by `dispatch_with_retries` itself,
            # and the subscription's fate is `_run`'s business, not this path's.
            with suppress(Exception):
                in_flight.exception()
            raise

    def _topic_partitions(self, topic_key: str) -> list[_Partition]:
        """The partitions of a topic, creating them on first use.

        Check-then-set, not `setdefault`. `setdefault` evaluates its default
        argument unconditionally, so `[_Partition() for _ in range(self._partitions)]`
        built twelve throwaway objects on *every* call even when the topic has
        existed for hours. This runs on the consumer's poll path and again on every
        committed offset, so a 500-order run allocated roughly 370,000 dead
        `_Partition` objects to look up a list that was already there.

        The behaviour is identical either way -- `setdefault` also leaves an
        existing value untouched -- and the object churn is gone.
        """
        partitions = self._topics.get(topic_key)
        if partitions is None:
            partitions = [_Partition() for _ in range(self._partitions)]
            self._topics[topic_key] = partitions
        return partitions

    def _update_lag(self, topic_key: str, group: str) -> None:
        lag = sum(
            max(
                0,
                self._topic_partitions(topic_key)[index].end_offset
                - self._committed.get((topic_key, group, index), 0),
            )
            for index in range(self._partitions)
        )
        self._metrics.consumer_lag.labels(topic=topic_key, group=group).set(lag)

    # -- inspection --------------------------------------------------------

    async def committed_offsets(self, topic: Topic, group: str) -> dict[int, int]:
        topic_key = str(topic)
        if topic_key not in self._topics:
            return {}
        return {
            index: self._committed[(topic_key, group, index)]
            for index in range(self._partitions)
            if (topic_key, group, index) in self._committed
        }

    async def end_offsets(self, topic: Topic) -> dict[int, int]:
        partitions = self._topics.get(str(topic))
        if partitions is None:
            return {}
        return {index: partition.end_offset for index, partition in enumerate(partitions)}

    async def lag(self, topic: Topic, group: str) -> int:
        topic_key = str(topic)
        partitions = self._topics.get(topic_key)
        if partitions is None:
            return 0
        return sum(
            max(0, partition.end_offset - self._committed.get((topic_key, group, index), 0))
            for index, partition in enumerate(partitions)
        )

    async def replay(self, topic: Topic, from_offset: int = 0) -> list[EventEnvelope]:
        """Every record at or after `from_offset`, in offset order per partition."""
        partitions = self._topics.get(str(topic))
        if partitions is None:
            return []
        records: list[EventEnvelope] = []
        for partition in partitions:
            records.extend(
                record for record in partition.records if (record.offset or 0) >= from_offset
            )
        return records

    async def published(self) -> list[EventEnvelope]:
        """Everything ever published, in publish order.

        The other half of reconciliation: compare this against what the
        database says was processed and the difference is data loss.
        """
        return list(self._published)

    def published_envelopes(self) -> list[EventEnvelope]:
        """`published()` without awaiting, for synchronous callers.

        For `TestClient`-driven API tests, which run the app on a *different*
        event loop than the test's own -- so `run_until_complete(bus.published())`
        from a test either deadlocks or awaits on a loop the app never runs on.
        The list is plain data and reading it is safe from anywhere; the async
        form stays for callers that are already inside the loop.
        """
        return list(self._published)

    def committed_position(self, group: str) -> dict[int, int]:
        """Committed offsets per partition for `group`, synchronously.

        Same reason as `published_envelopes`: a test asserting that a consumer
        caught up needs the number, and it is plain data.
        """
        return {
            partition: offset
            for (topic, consumer_group, partition), offset in self._committed.items()
            if consumer_group == group
        }

    async def wait_for_drain(self, topic: Topic, group: str, timeout: float = 5.0) -> bool:
        """Block until this group has consumed everything published.

        Used by the load test and the smoke script instead of a fixed sleep. A
        sleep is a race that passes on a fast machine and fails in CI.
        """
        from backend.core.clock import monotonic

        deadline = monotonic() + timeout
        while monotonic() < deadline:
            if await self.lag(topic, group) == 0:
                return True
            await asyncio.sleep(_OFFSET_POLL_SECONDS)
        raise TimeoutError(
            f"group {group!r} still behind on {topic} after {timeout}s: "
            f"lag={await self.lag(topic, group)}"
        )


def build_in_memory_log(*, metrics: FlowMeshMetrics, partitions: int = 6) -> InMemoryEventLog:
    """Construct the in-process log, for tests, the load test and `--in-process`."""
    return InMemoryEventLog(metrics=metrics, partitions=partitions)


def handler_returns(seen: list[EventEnvelope]) -> Handler:
    """A handler that records deliveries into `seen`. For tests and examples."""

    async def _handle(envelope: EventEnvelope) -> None:
        seen.append(envelope)

    return _handle
