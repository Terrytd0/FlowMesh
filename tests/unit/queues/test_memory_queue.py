"""The in-process task queue: ack, nack, redelivery, dead-letter.

The RabbitMQ port has to behave identically to this, and it is the
`dispatch`/`depth` contract that makes that testable. The three properties the
review worker depends on:

- A handler that returns **acks**; one that raises **redelivers**.
- Redelivery is bounded and then **dead-lettered**, not infinite. A poison
  message in front of the queue is the failure mode that stops a review process
  entirely.
- **Unacked work is not ready work.** `depth()` reports them separately, because
  a queue-depth gauge that counts in-flight messages as waiting reports a backlog
  that is not there.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from backend.observability.metrics import FlowMeshMetrics
from backend.queues.memory_queue import InMemoryTaskQueue, json_task, parse_task


@pytest.fixture
async def queue(metrics: FlowMeshMetrics) -> Any:
    instance = InMemoryTaskQueue(metrics=metrics, max_attempts=3)
    await instance.start()
    yield instance
    await instance.stop()


async def _run_until(predicate: Any, timeout: float = 3.0) -> None:
    """Poll until `predicate()` is truthy, or fail with a timeout.

    A fixed `sleep` would make these tests either slow or flaky. `predicate` may
    be sync or async; the coroutine form is awaited once per poll, so it must
    return a *fresh* coroutine each call rather than a stored one.
    """
    from backend.core.clock import monotonic

    deadline = monotonic() + timeout
    while monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"condition not met within {timeout}s")


def _dead_lettered(queue: Any, name: str) -> Any:
    async def _check() -> bool:
        return len(await queue.dead_letters(name)) == 1

    return _check


def _depth_is(queue: Any, name: str, **expected: int) -> Any:
    async def _check() -> bool:
        return await queue.depth(name) == expected

    return _check


def _ready_is(queue: Any, name: str, count: int) -> Any:
    async def _check() -> bool:
        depths = await queue.depth(name)
        return depths["ready"] == count

    return _check


# ---------------------------------------------------------------- delivery


async def test_a_published_task_is_handled(queue: InMemoryTaskQueue) -> None:
    seen: list[bytes] = []
    consumer = asyncio.create_task(queue.consume("q", _collect(seen)))
    await queue.publish("q", b"hello")
    await _run_until(lambda: seen == [b"hello"])
    await queue.stop()
    consumer.cancel()


async def test_tasks_are_handled_in_order(queue: InMemoryTaskQueue) -> None:
    seen: list[bytes] = []
    consumer = asyncio.create_task(queue.consume("q", _collect(seen)))
    for index in range(10):
        await queue.publish("q", f"task-{index}".encode())
    await _run_until(lambda: len(seen) == 10)
    assert seen == [f"task-{index}".encode() for index in range(10)]
    await queue.stop()
    consumer.cancel()


async def test_queues_are_isolated(queue: InMemoryTaskQueue) -> None:
    """A message for one queue must never be handed to another's consumer."""
    first: list[bytes] = []
    second: list[bytes] = []
    consumer_a = asyncio.create_task(queue.consume("a", _collect(first)))
    consumer_b = asyncio.create_task(queue.consume("b", _collect(second)))
    await queue.publish("a", b"for-a")
    await _run_until(lambda: first == [b"for-a"])
    await asyncio.sleep(0.05)
    assert second == []
    await queue.stop()
    consumer_a.cancel()
    consumer_b.cancel()


# ---------------------------------------------------------------- failures


async def test_a_failing_handler_redelivers(queue: InMemoryTaskQueue) -> None:
    """A raise is a nack, and the task comes back.

    This is the property that makes a review decision safe to retry: the worker
    either applied the decision and acked, or it did not and gets another go.
    """
    attempts = 0

    async def _flaky(body: bytes) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient")

    consumer = asyncio.create_task(queue.consume("q", _flaky))
    await queue.publish("q", b"retry-me")
    await _run_until(lambda: attempts >= 3)
    assert attempts == 3
    await queue.stop()
    consumer.cancel()


async def test_a_poison_task_is_dead_lettered(queue: InMemoryTaskQueue) -> None:
    """Bounded, then parked. Never infinite.

    A message that requeues forever blocks every review behind it, and the whole
    review process stops while every log line says the handler ran.
    """
    attempts = 0

    async def _always_fails(body: bytes) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("always")

    consumer = asyncio.create_task(queue.consume("q", _always_fails))
    await queue.publish("q", b"poison")
    await _run_until(lambda: attempts >= 3)
    await _run_until(_dead_lettered(queue, "q"))
    await asyncio.sleep(0.05)
    assert attempts == 3, "the handler kept being called after the attempt limit"
    assert await queue.dead_letters("q") == [b"poison"]
    await queue.stop()
    consumer.cancel()


async def test_a_poison_task_does_not_block_the_next_one(queue: InMemoryTaskQueue) -> None:
    """The queue keeps moving.

    Without this, one unparseable message would stop the review process for
    everyone, which is the failure a dead-letter queue exists to prevent.
    """
    handled: list[bytes] = []

    async def _selective(body: bytes) -> None:
        if body == b"poison":
            raise RuntimeError("cannot parse")
        handled.append(body)

    consumer = asyncio.create_task(queue.consume("q", _selective))
    await queue.publish("q", b"poison")
    await queue.publish("q", b"good-1")
    await _run_until(lambda: handled == [b"good-1"])
    await queue.stop()
    consumer.cancel()


async def test_dead_lettering_is_counted(
    queue: InMemoryTaskQueue, metrics: FlowMeshMetrics
) -> None:
    async def _always_fails(body: bytes) -> None:
        raise RuntimeError("no")

    consumer = asyncio.create_task(queue.consume("q", _always_fails))
    await queue.publish("q", b"poison")
    await _run_until(_dead_lettered(queue, "q"))
    assert metrics.value_of("flowmesh_queue_dead_lettered_total", queue="q") == 1.0
    await queue.stop()
    consumer.cancel()


# ---------------------------------------------------------------- depth


async def test_depth_reports_ready_unacked_and_dead_letter_separately(
    queue: InMemoryTaskQueue, metrics: FlowMeshMetrics
) -> None:
    """A gauge that counts in-flight messages as waiting reports a backlog that
    is not there."""
    for index in range(3):
        await queue.publish("q", f"t{index}".encode())
    assert await queue.depth("q") == {"ready": 3, "unacked": 0, "dead_letter": 0}
    assert metrics.value_of("flowmesh_queue_depth", queue="q", state="ready") == 3.0

    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow(body: bytes) -> None:
        started.set()
        await release.wait()

    consumer = asyncio.create_task(queue.consume("q", _slow))
    await asyncio.wait_for(started.wait(), timeout=2.0)
    assert await queue.depth("q") == {"ready": 2, "unacked": 1, "dead_letter": 0}
    assert metrics.value_of("flowmesh_queue_depth", queue="q", state="unacked") == 1.0

    release.set()
    await queue.stop()
    consumer.cancel()


async def test_depth_drains_to_zero(queue: InMemoryTaskQueue) -> None:
    handled: list[bytes] = []
    consumer = asyncio.create_task(queue.consume("q", _collect(handled)))
    for index in range(5):
        await queue.publish("q", f"t{index}".encode())
    await _run_until(_ready_is(queue, "q", 0))
    assert (await queue.depth("q"))["unacked"] == 0
    await queue.stop()
    consumer.cancel()


# ---------------------------------------------------------------- concurrency


async def test_concurrency_bounds_in_flight_work(queue: InMemoryTaskQueue) -> None:
    """`concurrency` is prefetch, not a request rate.

    Three workers must never have more than three messages in flight, which is
    what makes RabbitMQ's own prefetch semantics reproducible in a test.
    """
    in_flight = 0
    peak = 0
    release = asyncio.Event()

    async def _tracked(body: bytes) -> None:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await release.wait()
        in_flight -= 1

    consumer = asyncio.create_task(queue.consume("q", _tracked, concurrency=3))
    for index in range(10):
        await queue.publish("q", f"t{index}".encode())
    await asyncio.sleep(0.05)
    assert peak <= 3, f"concurrency=3 allowed {peak} in flight"
    release.set()
    await queue.stop()
    consumer.cancel()


# ---------------------------------------------------------------- bodies


def test_task_bodies_round_trip() -> None:
    body = json_task({"order_id": "CRG-1", "score": 0.72, "reasons": ["a", "b"]})
    assert isinstance(body, bytes)
    assert parse_task(body) == {"order_id": "CRG-1", "score": 0.72, "reasons": ["a", "b"]}


def test_a_corrupt_body_raises_rather_than_returning_junk() -> None:
    """The worker nacks on this, and the transport dead-letters it."""
    with pytest.raises(ValueError):
        parse_task(b"{not json")


def test_unserialisable_values_do_not_break_serialisation() -> None:
    """`default=str` so one odd value cannot fail the publish.

    A publish that raises on an unexpected type takes the order with it; a publish
    that coerces the value to its `str` loses a little precision in a log field
    and keeps the pipeline running.
    """
    from datetime import UTC, datetime

    body = json_task({"when": datetime(2026, 9, 29, 12, 0, tzinfo=UTC)})
    assert "2026-09-29" in parse_task(body)["when"]


# ---------------------------------------------------------------- metrics


async def test_publishing_and_processing_are_counted(
    queue: InMemoryTaskQueue, metrics: FlowMeshMetrics
) -> None:
    seen: list[bytes] = []
    consumer = asyncio.create_task(queue.consume("q", _collect(seen)))
    await queue.publish("q", b"x")
    await _run_until(lambda: seen == [b"x"])
    assert metrics.value_of("flowmesh_queue_published_total", queue="q") == 1.0
    assert metrics.value_of("flowmesh_queue_processed_total", queue="q", outcome="approved") == 1.0
    await queue.stop()
    consumer.cancel()


def _collect(sink: list[bytes]) -> Any:
    async def _handle(body: bytes) -> None:
        sink.append(body)

    return _handle
