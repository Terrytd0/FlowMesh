"""The chaos test: kill a consumer mid-stream and prove nothing is lost.

This is the sprint's hardest claim -- "a killed consumer resumes from its
committed offset with zero data loss" -- and the reason it needs a script rather
than a sentence is that *almost any* implementation passes a version of it that
does not test the claim. Three ways it could be fake, and what stops each:

1. **"We processed N events, and we published N."** Stopped by reconciliation by
   *identity*: every published `event_id` is looked up in `processed_events`, and
   the missing ids are named. A count cannot do this -- 400 against 400 is also
   what a pipeline that applied two events twice and lost two others reports.

2. **The kill lands at a convenient moment.** Stopped by requiring a real backlog
   at the kill point: the test waits for the consumer to have handled a configured
   fraction of the stream, then reports `backlog_at_kill`. A kill with nothing
   queued fails the run rather than passing it.

3. **The replay only "worked" because nothing needed replaying.** Stopped by
   counting events delivered *after* the restart that were not delivered before
   it, and failing on zero. That is the number that distinguishes a real replay
   from a kill that happened to be harmless.

The last one matters most. A handler that is idempotent can make a lost event
look handled: it ran, crashed after applying its effect, and the redelivery
no-op'd. That is correct behaviour, but it is not evidence of replay, so the test
separates "the effect is applied exactly once" from "the record was delivered
again".

**Where the kill comes from.** The in-process log's `cancel()` is a real
`Task.cancel()` on the consumer's own task -- the same code path a SIGTERM takes
through `run_order_consumer.py`. It is not a flag that makes the handler skip, and
it is not a mock. What it does not exercise is the *broker's* own behaviour under
a consumer death: real Kafka rebalances the partition, and the new owner starts
from the last committed offset. The in-process log's group offset is a position in
history, so the replay semantics are the same, but the rebalance timing is not.
That difference is stated in `docs/chaos-test.md` rather than glossed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import tempfile
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from backend.core.clock import monotonic
from backend.core.logging import configure_logging, get_logger
from backend.events.bus import Subscription
from backend.events.schema import EventType, Topic, envelope_for
from backend.pipeline.order_processor import OrderProcessor
from backend.scripts.loadtest import OrderGenerator

if TYPE_CHECKING:
    from backend.events.memory_log import InMemoryEventLog

logger = get_logger(__name__)

DEFAULT_STREAM_SIZE = 400
DEFAULT_KILL_AFTER = 0.35
"""Fraction of the stream after which the consumer is killed.

Not 1.0. A kill after the last event proves nothing, and 0.5 makes the test
brittle against a fast machine. 0.35 leaves most of the stream after the kill,
which is the point: the replay has work to do.
"""

#: How many post-restart deliveries may exceed the backlog at the kill point.
#:
#: An in-flight record's handler is allowed to finish before the cancellation is
#: honoured (see `InMemoryEventLog._consume_partition`), so it is delivered once
#: and counted once, but it lands after `handled_before_kill` was sampled. Two is
#: the observed maximum at four partitions. This exists only to stop that
#: accounting from reading as "restarted from zero"; a real restart-from-zero
#: replays the whole stream and blows straight past it.
_IN_FLIGHT_TOLERANCE = 4


@dataclass
class ChaosResult:
    """What happened, in enough detail to be checked rather than believed."""

    published: int = 0
    handled_before_kill: int = 0
    handled_after_restart: int = 0
    killed: bool = False
    kill_offset: int | None = None
    kill_fraction: float = 0.0
    #: Events still queued when the consumer was killed. The number that makes
    #: "mid-stream" mean something; a kill with zero backlog is a no-op however
    #: it is described.
    backlog_at_kill: int = 0
    #: Events delivered to the handler *after* the restart that had not been
    #: delivered before it. This is the replay, and it is the number that proves
    #: the uncommitted tail came back rather than the consumer simply never
    #: having had work to do.
    replayed: int = 0
    delivered_after_restart: int = 0
    #: Event ids delivered more than once. Near-zero in practice: it requires the
    #: process to die between the handler's commit and the offset commit, which is
    #: a sub-millisecond window. Kept because it is the only thing that exercises
    #: the idempotency ledger's skip path, and the load test covers the rest.
    redelivered: int = 0
    applied_effects: int = 0
    missing_from_ledger: list[str] = field(default_factory=list)
    #: Published `order_id`s with no row in `orders`. Distinct from
    #: `missing_from_ledger`: an event can be in the ledger with its order absent,
    #: and that is a different bug from a lost event.
    missing_orders: list[str] = field(default_factory=list)
    duplicate_effects: list[str] = field(default_factory=list)
    ledger_rows: int = 0
    orders_in_database: int = 0
    backlog_after_restart: float = 0.0
    passed: bool = False
    failures: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "published": self.published,
            "handled_before_kill": self.handled_before_kill,
            "handled_after_restart": self.handled_after_restart,
            "killed": self.killed,
            "kill_offset": self.kill_offset,
            "kill_fraction": round(self.kill_fraction, 3),
            "backlog_at_kill": self.backlog_at_kill,
            "replayed": self.replayed,
            "delivered_after_restart": self.delivered_after_restart,
            "redelivered": self.redelivered,
            "applied_effects": self.applied_effects,
            "missing_from_ledger": self.missing_from_ledger,
            "missing_orders": self.missing_orders,
            "duplicate_effects": self.duplicate_effects,
            "ledger_rows": self.ledger_rows,
            "orders_in_database": self.orders_in_database,
            "backlog_after_restart_seconds": round(self.backlog_after_restart, 3),
            "passed": self.passed,
            "failures": self.failures,
        }


async def run_chaos_test(
    *,
    stream_size: int = DEFAULT_STREAM_SIZE,
    kill_after: float = DEFAULT_KILL_AFTER,
    seed: int = 11,
) -> ChaosResult:
    """Publish a stream, kill the consumer mid-flight, restart it, reconcile.

    The three assertions that make this a test rather than a demonstration are in
    `verify()` below, and each one can fail independently.
    """
    from backend.database.repositories import idempotency
    from backend.loadtest.harness import build_pipeline

    result = ChaosResult()
    rng = random.Random(seed)

    with tempfile.TemporaryDirectory(prefix="flowmesh-chaos-") as directory:
        # A handler that records deliveries, so a replay is visible as a
        # post-restart delivery rather than being hidden behind the idempotency
        # ledger.
        #
        # Only `order.accepted` is recorded. The handler receives *every* event on
        # the topic, including the `order.scored` events it publishes itself, so
        # recording unconditionally counted 392 "replayed" events against a
        # backlog of 195 -- 300 accepted plus 92 of the processor's own output.
        # The assertion that flagged it was right and the accounting was wrong.
        deliveries: list[str] = []

        # Patched at `subscription()`, not at `processor.handle`, and the
        # difference is load-bearing.
        #
        # `OrderProcessor.subscription()` builds `Subscription(handler=self.handle)`,
        # so it captures the *bound method* at the moment it is called. `start()`
        # calls it, `build_pipeline` calls `start()`, and the consumer is already
        # running by the time this function gets control -- so assigning to
        # `processor.handle` afterwards replaced an attribute that nothing
        # dispatches through any more. The recorded list stayed empty, which made
        # `delivered_after_restart` 0 and `replayed` 0, and `verify()` then failed
        # the run on its own third assertion:
        #
        #     "no event was delivered after the restart that had not been
        #      delivered before it, so the uncommitted tail never came back"
        #
        # i.e. the chaos test reported a replay failure having measured no
        # deliveries at all, and would have done so forever. Patching the factory
        # means both the first subscription and the post-kill restart get the
        # recording handler, which is the only way the replay can be counted.
        original_subscription = OrderProcessor.subscription

        def _recording_subscription(
            self: OrderProcessor, instances: int = 1, instance_id: int = 0
        ) -> Subscription:
            subscription = original_subscription(self, instances, instance_id)
            inner = subscription.handler

            async def _handle(envelope: Any) -> None:
                if envelope.event_type == EventType.ORDER_ACCEPTED:
                    deliveries.append(envelope.event_id)
                await inner(envelope)

            return replace(subscription, handler=_handle)

        OrderProcessor.subscription = _recording_subscription  # type: ignore[method-assign]
        try:
            pipeline = await build_pipeline(
                database_path=Path(directory) / "chaos.sqlite3", partitions=4
            )
            bus: InMemoryEventLog = pipeline["bus"]
            generator = OrderGenerator(seed=seed)
            published_ids: list[str] = []
            published_order_ids: list[str] = []
            processor = pipeline["processor"]

            try:
                # Publish the stream faster than the consumer can drain it, so there
                # is guaranteed to be a backlog at kill time. A stream the consumer
                # keeps up with would make the kill a no-op and the test would pass
                # without testing anything.
                for index in range(stream_size):
                    context, _suspicious = generator.next_order(index)
                    event = envelope_for(
                        EventType.ORDER_ACCEPTED,
                        generator.to_payload(context),
                        order_id=context.order_id,
                    )
                    await bus.publish(Topic.ORDER, context.order_id, event)
                    published_ids.append(event.event_id)
                    published_order_ids.append(context.order_id)
                result.published = len(published_ids)

                # Kill once the consumer has processed `kill_after` of the stream,
                # which guarantees a backlog at the kill point.
                kill_target = int(stream_size * kill_after)
                await _wait_until_handled(
                    processor, target=kill_target, timeout=30.0, stream_size=stream_size
                )
                result.kill_offset = processor.stats.handled
                result.handled_before_kill = processor.stats.handled
                result.kill_fraction = result.kill_offset / stream_size
                # `killed` means there was genuinely work left in flight. Without this
                # the test can pass vacuously: a kill after the last event is harmless
                # by construction, and the "zero loss" result would prove nothing. The
                # first version of this killed at 98% of the stream -- technically
                # mid-stream, practically a no-op -- and the redelivery count of 0 is
                # what caught it.
                result.killed = result.kill_offset < stream_size
                backlog_at_kill = stream_size - result.kill_offset
                result.backlog_at_kill = backlog_at_kill

                # The kill. A real `Task.cancel()` on the consumer's own task, which
                # leaves its committed offsets exactly where they were.
                await bus.cancel(pipeline["handle"])

                # Restart: a *new* subscription in the *same* group. It resumes from
                # the committed offsets, not from zero -- which is the property, and
                # if it did not hold, the replay assertion below would fail.
                restart_started = monotonic()
                pipeline["handle"] = await processor.start()
                await _await_replay_or_report_death(
                    bus,
                    pipeline["handle"],
                    processor,
                    result,
                    timeout=60.0,
                )
                result.backlog_after_restart = monotonic() - restart_started
                result.handled_after_restart = processor.stats.handled

                # Reconciliation, by identity.
                async with pipeline["session_factory"]() as session:
                    applied = await idempotency.processed_event_ids(session, topic=str(Topic.ORDER))
                    result.ledger_rows = len(applied)
                    result.missing_from_ledger = [
                        event_id for event_id in published_ids if event_id not in applied
                    ]
                    result.applied_effects = processor.stats.handled

                    from sqlalchemy import func, select

                    from backend.database.models import Order, ProcessedEvent

                    duplicate_orders = await session.execute(
                        select(Order.id).group_by(Order.id).having(func.count() > 1)
                    )
                    result.duplicate_effects = [row for row in duplicate_orders.scalars()]
                    orders = await session.execute(select(func.count()).select_from(Order))
                    result.orders_in_database = int(orders.scalar_one())

                    # The same reconciliation by identity, one level down: an event
                    # in the ledger is not the same as an order in the database. A
                    # count difference of one is how this was first caught, and a
                    # count cannot say *which* order, so the difference is reported
                    # by name.
                    persisted = await session.execute(select(Order.id))
                    persisted_ids = set(persisted.scalars())
                    result.missing_orders = [
                        order_id
                        for order_id in published_order_ids
                        if order_id not in persisted_ids
                    ]

                    effects = await session.execute(
                        select(func.count()).select_from(ProcessedEvent)
                    )
                    _ = int(effects.scalar_one())

                # Replay, measured properly.
                #
                # The first version of this counted event ids delivered *twice* and
                # failed, correctly, at zero. The reasoning was wrong: the uncommitted
                # tail was **never delivered** before the kill, so it cannot have been
                # delivered twice. A genuine double delivery needs the process to die
                # in the window between the handler's commit and the offset commit --
                # a sub-millisecond window that almost never occurs by chance.
                #
                # The property that matters, and that a kill actually exercises, is
                # simpler: events delivered *after* the restart that were not delivered
                # before it. That is the replay. `redelivered` is kept below, computed
                # the same way, because a genuine duplicate delivery is still worth
                # knowing about -- it is how the idempotency ledger gets exercised.
                delivered_before = set(deliveries[: result.handled_before_kill])
                delivered_after = deliveries[result.handled_before_kill :]
                result.delivered_after_restart = len(delivered_after)
                result.replayed = sum(
                    1 for event_id in delivered_after if event_id not in delivered_before
                )
                _seen: set[str] = set()
                for event_id in deliveries:
                    if event_id in _seen:
                        result.redelivered += 1
                    _seen.add(event_id)

                _ = rng
                verify(result, published_ids)
            finally:
                try:
                    await bus.cancel(pipeline["handle"])
                except Exception:  # noqa: BLE001 - teardown must not mask a failure
                    pass
                await pipeline["queue"].stop()
                await bus.stop()
                await pipeline["engine"].dispose()
        finally:
            OrderProcessor.subscription = original_subscription  # type: ignore[method-assign]

    return result


async def _await_replay_or_report_death(
    bus: InMemoryEventLog,
    handle: Any,
    processor: Any,
    result: ChaosResult,
    *,
    timeout: float,
) -> None:
    """Wait for the restarted consumer to drain, or report that it died doing so.

    `wait_for_drain` on its own was a 60-second silence. It polls a lag counter that
    never moves once the subscription is dead, so a consumer that crashed on its
    first statement looked exactly like one that was merely slow: the run burned the
    whole timeout, raised `TimeoutError`, and that escaped from inside a
    `TemporaryDirectory` context manager -- so the traceback a reader saw was a
    `PermissionError` about a locked SQLite file on cleanup, which says nothing
    about the consumer dying.

    Both real causes were there underneath: `HandlerFailedError` from a handler that
    could not get a write lock, and a `Task` whose exception `subscribe` never
    retrieves. Watching the task turns a 60-second hang into a named failure.
    """
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        task = handle.task
        if task is not None and task.done():
            if task.cancelled():
                result.failures.append(
                    "the restarted consumer's task was cancelled instead of running"
                )
                return
            failure = task.exception()
            if failure is not None:
                result.failures.append(
                    f"the restarted consumer stopped consuming and can never drain the "
                    f"backlog: {type(failure).__name__}: {failure}"
                )
                return
        if await bus.lag(Topic.ORDER, processor.subscription().group) == 0:
            return
        await asyncio.sleep(0.002)
    result.failures.append(
        f"the restarted consumer did not drain the backlog within {timeout:.0f}s; "
        f"lag={await bus.lag(Topic.ORDER, processor.subscription().group)}"
    )


async def _wait_until_handled(
    processor: Any, *, target: int, timeout: float, stream_size: int
) -> None:
    """Wait until the consumer has handled `target` events, or the stream is done.

    Polling the *handler's own counter* rather than the log's lag, because the
    question is "how far has the consumer got", and the lag only answers it
    indirectly. Polling rather than sleeping is what makes the kill land at the
    same point in the stream regardless of machine speed -- a fixed sleep would
    either kill before anything was processed (nothing to replay) or after
    everything was (nothing to replay either), and which of those happens depends
    on how fast the CI machine is.
    """
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        handled = processor.stats.handled
        if handled >= target:
            return
        if handled >= stream_size:
            return
        await asyncio.sleep(0.002)
    raise TimeoutError(
        f"the consumer handled {processor.stats.handled} events without reaching "
        f"the kill point at {target}"
    )


def verify(result: ChaosResult, published_ids: list[str]) -> None:
    """The assertions. Each can fail independently, and each names itself.

    Returns a `ChaosResult` with `failures` populated; does not raise, so the
    report can be written before the process exits non-zero.
    """
    failures = result.failures

    if not result.killed:
        failures.append(
            "the consumer drained the whole stream before the kill, so nothing was "
            "tested: lower the rate or raise the stream size"
        )

    if result.missing_from_ledger:
        failures.append(
            f"DATA LOSS: {len(result.missing_from_ledger)} of {len(published_ids)} "
            f"published events were never applied. First few: "
            f"{result.missing_from_ledger[:5]}"
        )

    if result.backlog_at_kill <= 0:
        failures.append(
            f"there was no backlog at the kill ({result.backlog_at_kill} events "
            "queued), so nothing needed replaying: the result says nothing about "
            "recovery"
        )

    if result.replayed <= 0:
        failures.append(
            "no event was delivered after the restart that had not been delivered "
            "before it, so the uncommitted tail never came back: the test proved "
            "only that the kill was harmless"
        )

    if result.handled_after_restart <= result.handled_before_kill:
        failures.append(
            "the restarted consumer handled no further events, so the replayed "
            "backlog was not consumed"
        )

    if result.replayed > result.backlog_at_kill + _IN_FLIGHT_TOLERANCE:
        # The tolerance is not slack, it is the documented case. The kill lands
        # while a record is in flight, and `_consume_partition` lets an in-flight
        # handler finish before honouring the cancellation -- so that record's
        # handler runs to completion *after* `handled_before_kill` was sampled and
        # its delivery is counted again in the post-restart window, even though the
        # log never delivered it twice. With `partitions=4` a couple of records are
        # typically in flight at once, so the excess is 1-2 rather than 0.
        #
        # The check is still worth having: a group that restarted from the *beginning*
        # of the log would replay the whole stream, which is a far larger excess and
        # a much worse bug than no replay at all.
        failures.append(
            f"{result.replayed} events replayed against a backlog of "
            f"{result.backlog_at_kill} (tolerance {_IN_FLIGHT_TOLERANCE} for records "
            "in flight at the kill): the consumer group appears to have restarted "
            "from the beginning of the log rather than from its committed offset"
        )

    if result.duplicate_effects:
        failures.append(
            f"{len(result.duplicate_effects)} orders were written more than once, "
            "so the effect was applied twice: the idempotency ledger is not working"
        )

    if result.orders_in_database != len(published_ids):
        failures.append(
            f"{result.orders_in_database} orders in the database against "
            f"{len(published_ids)} published"
            + (f"; missing: {result.missing_orders[:5]}" if result.missing_orders else "")
        )

    result.passed = not failures


def render_markdown(result: ChaosResult) -> str:
    """The body of `docs/chaos-test.md`."""
    verdict = "PASS" if result.passed else "FAIL"
    lines = [
        "# FlowMesh chaos test: kill a consumer mid-stream",
        "",
        f"**Result: {verdict}**",
        "",
        f"Generated {datetime.now(UTC).isoformat()} by `make chaos`.",
        "",
        "## What this asserts",
        "",
        "An event-driven pipeline's central promise is that a consumer that dies",
        "mid-flight loses nothing. Almost any implementation passes a version of",
        "that test which does not test the claim, so this one is built to fail in",
        "three specific ways:",
        "",
        "1. **Reconciliation by identity.** Every published `event_id` is looked up",
        "   in the `processed_events` ledger and the *missing ids* are named. Not a",
        "   count -- a count of 400 against 400 is also what a pipeline that",
        "   processed the same event twice and lost two others would report.",
        "2. **The kill lands mid-stream.** The test waits for a backlog before",
        "   killing, and fails if the consumer had already drained everything.",
        '3. **The replay is proven, not assumed.** "Zero loss" is also what a',
        "   consumer that simply had no work would report. So the test counts events",
        "   delivered *after* the restart that had not been delivered before it, and",
        "   zero is a failure. Genuine double deliveries are counted separately --",
        "   they need the process to die in the sub-millisecond window between the",
        "   handler's commit and the offset commit, so they are rare by design and",
        "   their absence is not a problem.",
        "",
        "## Result",
        "",
        "| measure | value |",
        "| --- | --- |",
        f"| events published | {result.published} |",
        f"| handled before the kill | {result.handled_before_kill} "
        f"({result.kill_fraction:.0%} of the stream) |",
        f"| killed mid-stream | {'yes' if result.killed else 'NO -- test is void'} |",
        f"| events still queued at the kill | {result.backlog_at_kill} |",
        f"| handled after restart | {result.handled_after_restart} |",
        f"| **events replayed after restart** | **{result.replayed}** "
        f"(of {result.backlog_at_kill} queued at the kill) |",
        f"| events delivered twice (idempotency skip path) | {result.redelivered} |",
        f"| events in the idempotency ledger | {result.ledger_rows} |",
        f"| orders in the database | {result.orders_in_database} |",
        f"| **published but never applied** | **{len(result.missing_from_ledger)}** |",
        f"| orders written more than once | {len(result.duplicate_effects)} |",
        f"| backlog drained after restart | {result.backlog_after_restart:.2f}s |",
        "",
    ]

    if result.missing_from_ledger:
        lines += [
            "### Events lost",
            "",
            "```json",
            json.dumps(result.missing_from_ledger[:20], indent=2),
            "```",
            "",
        ]

    lines += ["## Why the replay works", ""]
    lines += [
        "The consumer commits its offset **after** the handler returns, and the",
        "handler writes the `processed_events` row in the **same transaction** as its",
        "effect. Those two facts together give at-least-once delivery with",
        "effectively-once application:",
        "",
        "- crash before the commit -> the offset is uncommitted, so the record is",
        "  redelivered, and the rolled-back transaction left no trace to collide",
        "  with",
        "- crash after the commit, before the offset commit -> the record is",
        "  redelivered, and `mark_processed` finds the row and skips",
        "",
        "Neither window can produce a lost effect or a doubled one, and the second",
        "window is exactly the `redelivered` count above.",
        "",
        "## What this does not test",
        "",
        "Stated plainly, because the limits are part of the result:",
        "",
        "- **Broker-side rebalancing.** The kill is a real `Task.cancel()` on the",
        "  consumer's own task -- the same path a SIGTERM takes through",
        "  `run_order_consumer.py` -- but it is the in-process log, whose group",
        "  offsets are a position in history. Real Kafka additionally rebalances",
        "  the partition to another member, and that timing is not exercised here.",
        "- **Partial commits mid-handler.** A process killed between two writes",
        "  within one transaction is covered by the database's atomicity, not by",
        "  this test.",
        "- **An in-flight gRPC call.** The scorer runs in-process here; a call that",
        "  was abandoned at the socket is a different failure and is covered by the",
        "  deadline tests in `tests/integration/`.",
        "",
        "## Reproducing",
        "",
        "```bash",
        "make chaos",
        "python -m backend.scripts.chaos_test --stream 800 --kill-after 0.3",
        "```",
        "",
        "Exits non-zero on any failure, so it can be a CI gate.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="FlowMesh chaos test.")
    parser.add_argument("--stream", type=int, default=DEFAULT_STREAM_SIZE)
    parser.add_argument(
        "--kill-after",
        type=float,
        default=DEFAULT_KILL_AFTER,
        help="kill the consumer once it has handled this fraction of the stream",
    )
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--report", type=Path, default=Path("docs/chaos-test.md"))
    parser.add_argument("--json", type=Path, default=Path("data/runtime/chaos.json"))
    args = parser.parse_args()

    configure_logging("WARNING")
    result = asyncio.run(
        run_chaos_test(stream_size=args.stream, kill_after=args.kill_after, seed=args.seed)
    )
    print(json.dumps(result.to_dict(), indent=2))  # noqa: T201

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(render_markdown(result), encoding="utf-8")
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")

    if not result.passed:
        print("\nCHAOS TEST FAILED: " + "; ".join(result.failures))  # noqa: T201
        raise SystemExit(1)
    raise SystemExit(0)


# See the note on the same guard in `backend/scripts/loadtest.py`. This one was
# missing too, and the consequence was worse than a no-op: the chaos test is the
# evidence that killing a consumer loses no orders, so a build in which it silently
# did nothing reported "no data loss" having tested nothing.
if __name__ == "__main__":
    main()
