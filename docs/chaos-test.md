# FlowMesh chaos test: kill a consumer mid-stream

**Result: PASS**

Generated 2026-10-02T06:04:59.481837+00:00 by `make chaos`.

## What this asserts

An event-driven pipeline's central promise is that a consumer that dies
mid-flight loses nothing. Almost any implementation passes a version of
that test which does not test the claim, so this one is built to fail in
three specific ways:

1. **Reconciliation by identity.** Every published `event_id` is looked up
   in the `processed_events` ledger and the *missing ids* are named. Not a
   count -- a count of 400 against 400 is also what a pipeline that
   processed the same event twice and lost two others would report.
2. **The kill lands mid-stream.** The test waits for a backlog before
   killing, and fails if the consumer had already drained everything.
3. **The replay is proven, not assumed.** "Zero loss" is also what a
   consumer that simply had no work would report. So the test counts events
   delivered *after* the restart that had not been delivered before it, and
   zero is a failure. Genuine double deliveries are counted separately --
   they need the process to die in the sub-millisecond window between the
   handler's commit and the offset commit, so they are rare by design and
   their absence is not a problem.

## Result

| measure | value |
| --- | --- |
| events published | 400 |
| handled before the kill | 140 (35% of the stream) |
| killed mid-stream | yes |
| events still queued at the kill | 260 |
| handled after restart | 400 |
| **events replayed after restart** | **262** (of 260 queued at the kill) |
| events delivered twice (idempotency skip path) | 4 |
| events in the idempotency ledger | 400 |
| orders in the database | 400 |
| **published but never applied** | **0** |
| orders written more than once | 0 |
| backlog drained after restart | 7.90s |

## Why the replay works

The consumer commits its offset **after** the handler returns, and the
handler writes the `processed_events` row in the **same transaction** as its
effect. Those two facts together give at-least-once delivery with
effectively-once application:

- crash before the commit -> the offset is uncommitted, so the record is
  redelivered, and the rolled-back transaction left no trace to collide
  with
- crash after the commit, before the offset commit -> the record is
  redelivered, and `mark_processed` finds the row and skips

Neither window can produce a lost effect or a doubled one, and the second
window is exactly the `redelivered` count above.

## What this does not test

Stated plainly, because the limits are part of the result:

- **Broker-side rebalancing.** The kill is a real `Task.cancel()` on the
  consumer's own task -- the same path a SIGTERM takes through
  `run_order_consumer.py` -- but it is the in-process log, whose group
  offsets are a position in history. Real Kafka additionally rebalances
  the partition to another member, and that timing is not exercised here.
- **Partial commits mid-handler.** A process killed between two writes
  within one transaction is covered by the database's atomicity, not by
  this test.
- **An in-flight gRPC call.** The scorer runs in-process here; a call that
  was abandoned at the socket is a different failure and is covered by the
  deadline tests in `tests/integration/`.

## Reproducing

```bash
make chaos
python -m backend.scripts.chaos_test --stream 800 --kill-after 0.3
```

Exits non-zero on any failure, so it can be a CI gate.
