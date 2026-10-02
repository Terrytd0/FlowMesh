"""The in-process event log: offsets, groups, ordering, replay.

These are the tests the sprint's central claim rests on. "A killed consumer
resumes from its committed offset with zero data loss" is only meaningful if the
log actually behaves like a partitioned log, and a dict of mock calls cannot
demonstrate that -- there are no offsets to lose.

Every test here runs with nothing else started. That is the point of ADR-005.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import ValidationError

from backend.core.ids import new_event_id
from backend.events.bus import ConsumerHandle, HandlerFailedError, assign_partitions, partition_for
from backend.events.memory_log import InMemoryEventLog, handler_returns
from backend.events.schema import EventEnvelope, EventType, Topic
from backend.observability.metrics import FlowMeshMetrics


def order_event(order_id: str, event_type: EventType = EventType.ORDER_ACCEPTED) -> EventEnvelope:
    return EventEnvelope(event_type=event_type, order_id=order_id)


@pytest.fixture
def log(metrics: FlowMeshMetrics) -> InMemoryEventLog:
    return InMemoryEventLog(metrics=metrics, partitions=6)


# ---------------------------------------------------------------- offsets


async def test_publish_returns_a_monotonic_offset(log: InMemoryEventLog) -> None:
    offsets = [await log.publish(Topic.ORDER, "order-1", order_event("order-1")) for _ in range(5)]
    assert offsets == [0, 1, 2, 3, 4]


async def test_offsets_are_per_partition(log: InMemoryEventLog) -> None:
    """Two keys hashing to different partitions get independent offset spaces.

    This is what makes per-key ordering possible: all of one order's events land
    on one partition, so one consumer sees them in order, and a slow order does
    not hold up an unrelated one.
    """
    first = partition_for("order-1", 6)
    second = partition_for("order-2", 6)
    if first == second:
        pytest.skip("the two chosen keys happen to share a partition")
    assert (await log.publish(Topic.ORDER, "order-1", order_event("o1"))) == 0
    assert (await log.publish(Topic.ORDER, "order-2", order_event("o2"))) == 0


async def test_same_key_always_lands_on_one_partition(log: InMemoryEventLog) -> None:
    """Critical for ordering: the partitioner must be stable across processes.

    Python's built-in `hash()` is randomised per process, so a naive partitioner
    would put one order's events on two consumers the moment the fleet scaled past
    one process -- and per-key ordering would be quietly gone.
    """
    assert partition_for("order-abc", 6) == partition_for("order-abc", 6)
    assert partition_for("order-abc", 6) == partition_for("order-abc", 12) % 6


def test_partition_rejects_zero_partitions() -> None:
    with pytest.raises(ValueError, match="partitions"):
        partition_for("k", 0)


async def test_partition_assignment_is_round_robin(log: InMemoryEventLog) -> None:
    assert assign_partitions(6, 3, 0) == [0, 3]
    assert assign_partitions(6, 3, 1) == [1, 4]
    assert assign_partitions(6, 3, 2) == [2, 5]
    assert assign_partitions(6, 1, 0) == [0, 1, 2, 3, 4, 5]


def test_assignment_rejects_impossible_configuration() -> None:
    with pytest.raises(ValueError, match="instance_id"):
        assign_partitions(6, 2, 2)
    with pytest.raises(ValueError, match="instances"):
        assign_partitions(6, 0, 0)


async def test_a_subscription_gets_only_its_assigned_partitions(log: InMemoryEventLog) -> None:
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(
        _subscription(Topic.ORDER, "g", handler_returns(seen), instances=3, instance_id=1)
    )
    assert handle.assigned_partitions == [1, 4]
    await log.cancel(handle)
    assert seen == []


# ---------------------------------------------------------------- delivery


async def test_a_published_event_is_delivered(log: InMemoryEventLog) -> None:
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(seen)))
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))
    await _wait_until(lambda: len(seen) == 1)
    assert seen[0].order_id == "order-1"
    # The log, not the producer, assigns the offset and partition.
    assert seen[0].offset == 0
    assert seen[0].partition is not None
    await log.cancel(handle)


async def test_events_are_delivered_in_publish_order_within_a_key(
    log: InMemoryEventLog,
) -> None:
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(seen)))
    for _ in range(20):
        await log.publish(Topic.ORDER, "order-1", order_event("order-1", EventType.ORDER_SCORED))
    await _wait_until(lambda: len(seen) == 20)
    # The log assigns every offset, so `None` here would mean an envelope escaped
    # the log unassigned -- and `sorted` on a list containing `None` raises rather
    # than ordering, which would be a confusing way to find that out.
    offsets = [event.offset for event in seen]
    assert all(offset is not None for offset in offsets), "an event escaped without an offset"
    assert offsets == sorted(offset for offset in offsets if offset is not None)
    await log.cancel(handle)


async def test_groups_are_isolated(log: InMemoryEventLog) -> None:
    """Two groups each see every event.

    If they did not, one of them would be silently missing work -- and since
    nothing errors, that failure would only appear as a dashboard that stops
    moving.
    """
    fraud_seen: list[EventEnvelope] = []
    inventory_seen: list[EventEnvelope] = []
    fraud = await log.subscribe(
        _subscription(Topic.ORDER, "fraud-group", handler_returns(fraud_seen))
    )
    inventory = await log.subscribe(
        _subscription(Topic.ORDER, "inventory-group", handler_returns(inventory_seen))
    )
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))
    await _wait_until(lambda: len(fraud_seen) == 1 and len(inventory_seen) == 1)
    await log.cancel(fraud)
    await log.cancel(inventory)


async def test_a_second_member_of_a_group_sees_nothing_already_handled(
    log: InMemoryEventLog,
) -> None:
    """The property that makes consumer groups a queue rather than a broadcast.

    A group is a *position in history*, not a subscription. If a second consumer
    joining the same group re-read the log from zero, every order would be
    processed once per consumer -- a rebalance bug that looks like a data
    corruption bug.
    """
    first: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(first)))
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))
    await _wait_until(lambda: len(first) == 1)

    second: list[EventEnvelope] = []
    rejoined = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(second)))
    await _sleep(0.05)
    assert second == [], "a re-joining member of the group replayed committed records"
    await log.cancel(handle)
    await log.cancel(rejoined)


async def test_two_instances_of_a_group_split_the_partitions(log: InMemoryEventLog) -> None:
    """Scaling out means more throughput, not more deliveries."""
    left: list[EventEnvelope] = []
    right: list[EventEnvelope] = []
    first = await log.subscribe(
        _subscription(Topic.ORDER, "g", handler_returns(left), instances=2, instance_id=0)
    )
    second = await log.subscribe(
        _subscription(Topic.ORDER, "g", handler_returns(right), instances=2, instance_id=1)
    )
    for index in range(30):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))
    await _wait_until(lambda: len(left) + len(right) == 30, timeout=5.0)
    # Every record exactly once across the two instances.
    ids = [event.event_id for event in left] + [event.event_id for event in right]
    assert len(ids) == len(set(ids)) == 30
    await log.cancel(first)
    await log.cancel(second)


# ---------------------------------------------------------------- failures


async def test_a_failing_handler_retries_then_stops_the_subscription(
    log: InMemoryEventLog,
) -> None:
    """Bounded retries, then stop -- never commit past an unprocessable record.

    Committing past it would be silent data loss: the offset advances, the record
    is never handled again, and nothing anywhere reports an error.
    """
    attempts = 0

    async def _always_fails(event: EventEnvelope) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("handler is broken")

    handle = await log.subscribe(_subscription(Topic.ORDER, "g", _always_fails))
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))
    await _wait_until(lambda: attempts >= 3, timeout=5.0)
    await _sleep(0.1)
    # Three attempts, then it stops. Not a thousand.
    assert attempts == 3
    assert (await log.committed_offsets(Topic.ORDER, "g")).get(partition_for("order-1", 6), 0) == 0
    await log.cancel(handle)


async def test_a_recovered_handler_succeeds_on_retry(log: InMemoryEventLog) -> None:
    """The retry path is not just for permanent failures."""
    attempts = 0
    seen: list[EventEnvelope] = []

    async def _flaky(event: EventEnvelope) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise RuntimeError("transient")
        seen.append(event)

    handle = await log.subscribe(_subscription(Topic.ORDER, "g", _flaky))
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))
    await _wait_until(lambda: len(seen) == 1, timeout=5.0)
    assert attempts == 2
    await log.cancel(handle)


# ---------------------------------------------------------------- replay


async def test_replay_returns_records_ignoring_group_offsets(log: InMemoryEventLog) -> None:
    """The reconciliation primitive.

    `replay()` must not consult group state. If it did, it would skip exactly the
    records the reconciliation exists to find missing.

    All five records share one key, so they share a partition and their offsets
    are 0..4. That is what makes the `from_offset` assertion meaningful: offsets
    are *per partition*, so five records spread over six partitions would each sit
    at offset 0 and a `from_offset=3` filter would correctly return none of them.
    """
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(seen)))
    for _ in range(5):
        await log.publish(Topic.ORDER, "the-same-key", order_event("order-1"))
    await _wait_until(lambda: len(seen) == 5)
    await log.cancel(handle)

    assert len(await log.replay(Topic.ORDER)) == 5
    assert len(await log.replay(Topic.ORDER, from_offset=3)) == 2


async def test_replay_from_a_high_offset_on_sparse_partitions_is_empty(
    log: InMemoryEventLog,
) -> None:
    """Documents that `from_offset` is per partition, not a global cursor.

    Five distinct keys across six partitions leave most partitions holding a
    single record at offset 0. Asking for offset 3 finds nothing -- which is the
    correct answer, and the reason `scripts/chaos_test.py` reconciles by
    *identity* (event ids) rather than by seeking to a global offset.
    """
    for index in range(5):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))
    assert len(await log.replay(Topic.ORDER)) == 5
    assert await log.replay(Topic.ORDER, from_offset=3) == []


async def test_replay_on_an_empty_topic_is_empty(log: InMemoryEventLog) -> None:
    assert await log.replay(Topic.INVENTORY) == []


async def test_published_records_match_replayed_records(log: InMemoryEventLog) -> None:
    """The publisher's view and the reader's view must agree."""
    for index in range(10):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))
    published = {event.event_id for event in await log.published()}
    replayed = {event.event_id for event in await log.replay(Topic.ORDER)}
    assert published == replayed


async def test_lag_reports_uncommitted_records(log: InMemoryEventLog) -> None:
    for index in range(4):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))
    assert await log.lag(Topic.ORDER, "g") == 4
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(seen)))
    await _wait_until(_lag_is_zero(log), timeout=5.0)
    await log.cancel(handle)


async def test_wait_for_drain_returns_when_caught_up(log: InMemoryEventLog) -> None:
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(seen)))
    for index in range(25):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))
    assert await log.wait_for_drain(Topic.ORDER, "g", timeout=5.0)
    assert len(seen) == 25
    await log.cancel(handle)


async def test_wait_for_drain_times_out_when_nothing_is_consuming(
    log: InMemoryEventLog,
) -> None:
    """A hang is the worst possible failure for a test helper, so it raises.

    Records are published and no consumer is subscribed, so the lag stays above
    zero and the helper must give up rather than block a test run forever.
    """
    for index in range(3):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))
    with pytest.raises(TimeoutError, match="nobody-is-reading"):
        await log.wait_for_drain(Topic.ORDER, "nobody-is-reading", timeout=0.1)


async def test_wait_for_drain_returns_immediately_on_an_empty_topic(
    log: InMemoryEventLog,
) -> None:
    """No records and no consumer is lag zero, not a hang.

    The distinction from the test above is that this one is genuinely caught up.
    Treating "nothing has ever been published" as a timeout would make every
    empty-database check slow.
    """
    assert await log.wait_for_drain(Topic.ORDER, "g", timeout=0.1)


async def test_cancel_is_idempotent(log: InMemoryEventLog) -> None:
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns([])))
    await log.cancel(handle)
    await log.cancel(handle)


async def test_stop_cancels_every_subscription(log: InMemoryEventLog) -> None:
    handles: list[ConsumerHandle] = [
        await log.subscribe(_subscription(Topic.ORDER, f"g{index}", handler_returns([])))
        for index in range(3)
    ]
    await log.stop()
    assert all(handle.task is None or handle.task.done() for handle in handles)


# ------------------------------------------------------- cancelling safely


async def test_cancel_lets_an_in_flight_handler_finish(log: InMemoryEventLog) -> None:
    """A kill must not tear a handler's database transaction in half.

    `Task.cancel()` delivers a `CancelledError` at the handler's next await. If that
    await is a SQL statement, the cancellation does not stop the statement -- `aiosqlite`
    runs it on a worker thread that keeps going -- so the coroutine unwinds while the
    statement is still executing, the rollback is awaited from an already-cancelled
    context and never completes, and the connection goes back to neither the pool nor
    a rolled-back state. It keeps holding the store's write lock.

    The replacement consumer then starts while that lock is held and dies on its first
    statement with `database is locked` -- an error `busy_timeout` cannot rescue,
    because a deferred transaction that already holds a SHARED lock cannot wait out an
    upgrade. That is how the chaos test went from "zero loss" to a 60-second hang and
    then a `PermissionError` about a locked SQLite file.

    So `cancel()` honours the cancellation *between* records: the in-flight handler
    runs to completion, and only then does the consumer stop. The kill is still real
    -- the in-flight record's offset stays uncommitted -- but no transaction is torn.
    """
    finished: list[str] = []
    in_flight = asyncio.Event()

    async def slow_handler(envelope: EventEnvelope) -> None:
        in_flight.set()
        await asyncio.sleep(0.05)
        finished.append(envelope.order_id or "")

    handle = await log.subscribe(_subscription(Topic.ORDER, "g", slow_handler))
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))

    # Synchronise on the handler actually being *inside* its work, rather than
    # sleeping and hoping. A fixed sleep here is the same race the harness's own
    # `_wait_until_handled` exists to avoid.
    await asyncio.wait_for(in_flight.wait(), timeout=5.0)
    await log.cancel(handle)

    assert finished == ["order-1"], (
        "the in-flight handler was abandoned rather than allowed to finish, so its "
        "transaction may never have been rolled back"
    )


async def test_cancel_commits_no_offset_past_the_point_of_the_kill(
    log: InMemoryEventLog,
) -> None:
    """A record in flight when the kill lands is redelivered, not silently skipped.

    This is the exact boundary, and getting it wrong in either direction loses data.

    The cancellation is honoured *after* the in-flight handler finishes but *before*
    the commit line, so:

    - the handler's effect is applied, and its offset stays uncommitted;
    - the record therefore comes back on restart, where the idempotency ledger finds
      the event already applied and skips it -- at-least-once delivery with
      effectively-once effect, which is the whole design;
    - nothing behind it is lost. The lag after the kill equals the full stream.

    The tempting wrong answers are both real bugs. Committing the finished record's
    offset while abandoning the rest would drop the backlog; committing everything
    would mark unprocessed records as done. The first version of `_dispatch_one`
    did neither, but the test above caught it abandoning the handler outright.
    """
    seen: list[str] = []
    in_flight = asyncio.Event()

    async def slow_handler(envelope: EventEnvelope) -> None:
        in_flight.set()
        await asyncio.sleep(0.05)
        seen.append(envelope.order_id or "")

    handle = await log.subscribe(_subscription(Topic.ORDER, "g", slow_handler))
    for index in range(4):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))

    await asyncio.wait_for(in_flight.wait(), timeout=5.0)
    await log.cancel(handle)

    assert seen, "the in-flight handler did not run to completion"
    committed = sum(log.committed_position("g").values())
    assert committed == 0, (
        f"{committed} offsets were committed across the kill; a record whose handler "
        "had not been dispatched yet has been marked done and will never be redelivered"
    )
    assert await log.lag(Topic.ORDER, "g") == 4, (
        "records were lost across the kill: the whole stream must still be outstanding"
    )


# ---------------------------------------------------------------- metrics


async def test_publishing_and_consuming_are_counted(
    log: InMemoryEventLog, metrics: FlowMeshMetrics
) -> None:
    """Every publish moves the counter -- this is the number a dashboard reads."""
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(seen)))
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))
    await _wait_until(lambda: len(seen) == 1)
    assert metrics.value_of("flowmesh_events_published_total", topic="order-events") == 1.0
    assert (
        metrics.value_of(
            "flowmesh_events_consumed_total", topic="order-events", group="g", outcome="approved"
        )
        == 1.0
    )
    await log.cancel(handle)


async def test_redeliveries_are_counted(log: InMemoryEventLog, metrics: FlowMeshMetrics) -> None:
    attempts = 0

    async def _flaky(event: EventEnvelope) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("once")

    handle = await log.subscribe(_subscription(Topic.ORDER, "g", _flaky))
    await log.publish(Topic.ORDER, "order-1", order_event("order-1"))
    await _wait_until(lambda: attempts >= 2, timeout=5.0)
    assert (
        metrics.value_of("flowmesh_events_redelivered_total", topic="order-events", group="g")
        == 1.0
    )
    await log.cancel(handle)


async def test_consumer_lag_gauge_follows_the_log(
    log: InMemoryEventLog, metrics: FlowMeshMetrics
) -> None:
    seen: list[EventEnvelope] = []
    handle = await log.subscribe(_subscription(Topic.ORDER, "g", handler_returns(seen)))
    for index in range(3):
        await log.publish(Topic.ORDER, f"order-{index}", order_event(f"order-{index}"))
    await _wait_until(_lag_is_zero(log), timeout=5.0)
    assert metrics.value_of("flowmesh_consumer_lag", topic="order-events", group="g") == 0.0
    await log.cancel(handle)


# ---------------------------------------------------------------- envelope


def test_envelope_round_trips_through_json() -> None:
    """A schema that does not round-trip breaks consumers on a version bump."""
    original = EventEnvelope(event_type=EventType.ORDER_ACCEPTED, order_id="order-1")
    restored = EventEnvelope.from_wire(original.to_wire())
    assert restored.event_id == original.event_id
    assert restored.event_type == original.event_type
    assert restored.order_id == original.order_id
    assert restored.occurred_at == original.occurred_at


def test_envelope_is_frozen() -> None:
    """An envelope is a fact. Mutating one in flight is how a fact becomes a rumour.

    `pydantic.ValidationError` specifically, not a bare `Exception`: the point of
    the assertion is that the *type* is checked, and `pytest.raises(Exception)`
    would pass just as happily if the model were deleted.
    """
    event = EventEnvelope(event_type=EventType.ORDER_ACCEPTED)
    with pytest.raises(ValidationError, match="frozen"):
        event.order_id = "mutated"  # type: ignore[misc]


def test_unknown_event_type_is_rejected_on_parse() -> None:
    """An unrecognised `event_type` is a validation error, not a pass.

    This is the consumer-side schema check: an envelope whose `event_type` is not
    in the enum would otherwise arrive as a string and sail through to a handler
    that has never heard of it.
    """
    with pytest.raises(ValidationError):
        EventEnvelope.from_wire(
            '{"event_id":"x","event_type":"not.a.real.type","occurred_at":"2026-01-01T00:00:00Z"}'
        )


def test_every_event_type_has_a_distinct_value() -> None:
    values = [str(event) for event in EventType]
    assert len(values) == len(set(values))


def test_event_ids_are_unique() -> None:
    ids = {new_event_id() for _ in range(1000)}
    assert len(ids) == 1000


# ---------------------------------------------------------------- helpers


def _subscription(
    topic: Topic,
    group: str,
    handler: Any,
    *,
    instances: int = 1,
    instance_id: int = 0,
) -> Any:
    from backend.events.bus import Subscription

    return Subscription(
        topic=topic,
        group=group,
        handler=handler,
        instances=instances,
        instance_id=instance_id,
    )


async def _wait_until(predicate: Any, timeout: float = 2.0) -> None:
    """Wait for a condition, or fail the test.

    A fixed `sleep` would make these tests either slow or flaky; polling for the
    actual condition makes them neither. It raises rather than returning, so a
    timeout names the condition that never happened.

    `predicate` is a *factory*: it is called afresh on each poll, so an async
    condition is re-evaluated rather than awaiting one coroutine repeatedly. That
    is the only form that works for every case -- awaiting a single coroutine in a
    loop raises "cannot reuse already awaited coroutine", and passing a coroutine
    where a callable is expected raises "coroutine object is not callable".
    """
    from backend.core.clock import monotonic

    deadline = monotonic() + timeout
    while monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await _sleep(0.005)
    raise AssertionError(f"condition not met within {timeout}s")


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _lag_is_zero(log: InMemoryEventLog) -> Any:
    """A predicate factory: `await log.lag(...)` cannot live inside a lambda.

    Returns the coroutine, and `_wait_until` awaits it -- once per poll, from a
    fresh call each time.
    """

    async def _check() -> bool:
        return await log.lag(Topic.ORDER, "g") == 0

    return _check


def test_handler_failed_error_names_the_location() -> None:
    """The error has to say where, or a redelivery loop is undebuggable."""
    error = HandlerFailedError(
        topic="order-events", group="g", partition=2, offset=17, cause=RuntimeError("x")
    )
    text = str(error)
    assert "order-events" in text
    assert "g" in text
    assert "2" in text and "17" in text
    assert "RuntimeError" in text
