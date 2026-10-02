# ADR-005: the in-process log is an implementation, not a mock

**Status:** accepted
**Date:** 2026-09-29
**Context:** Sprint 7 — FlowMesh

## The decision

`backend/events/memory_log.py` and `backend/queues/memory_queue.py` are real
implementations of the `EventBus` and `TaskQueue` ports, sitting alongside the
Kafka and RabbitMQ ones. They are what `pytest` runs against, what the load test
runs, and what `FLOWMESH_EVENT_TRANSPORT=memory` selects.

This ADR records why, and — more usefully — exactly where the in-process
implementation differs from the real one. A test double that is *nearly* faithful
is worse than an obviously-fake one, because tests written against the difference
pass locally and fail in production.

## Why not mocks

The central claim of this sprint is: *a killed consumer resumes from its committed
offset with zero data loss*. Testing that against a mock is not possible, because
a mock has no offsets to lose. What you end up asserting is that
`handler.call_count == 1` — which is a test of the test double.

The three properties that make the chaos test meaningful are all properties of the
*log*, not of the handler:

1. **Records land in partitions with monotonic offsets.** Per-key ordering follows.
2. **Each `(topic, group, partition)` has its own committed position**, and
   publishing does not advance it. That is what makes a consumer group a queue
   rather than a broadcast.
3. **`replay()` reads from an offset, ignoring group state.** Without it there is
   nothing to reconcile against, and reconciliation by count proves nothing.

A mock provides none of the three. So `InMemoryEventLog` implements them: real
offsets, real groups, real redelivery, real replay.

## The same argument for the queue

`InMemoryTaskQueue` has per-message ack/nack, bounded redelivery, a dead-letter
queue, and separate `ready`/`unacked`/`dead_letter` depths. Those are exactly the
properties `ReviewWorker` depends on, and mocking them would leave the worker's
most important behaviour — treating a redelivered decision as already applied —
untested.

There is a specific bug this caught. The dead-letter writer used the bare queue name
as its key while both readers used `f"{queue}.dlq"`, so `depth()["dead_letter"]`
read 0 while the counter said 1. During exactly the incident where a poison message
was parked, the queue looked empty. No mock would have found that, because a mock
has no storage to disagree with itself.

## Where it deliberately differs, and what that forbids

These are the load-bearing limitations. A test must not depend on them.

**1. Partition assignment is static round-robin (`p % instances`).**
Kafka's cooperative-sticky assignor moves partitions on membership change; this
does not. Tests may assert that a two-instance group splits partitions evenly. They
may **not** assert anything about what happens when a member joins mid-stream — a
real broker would rebalance and this would not, and a test written against the
in-process behaviour would encode the wrong thing.

**2. `ensure_topics()` is a no-op.** Topics appear on first publish. A test cannot
assert topic-creation failure.

**3. Records are retained forever.** Kafka expires them on a retention policy. Any
test asserting "old events are gone after N days" is testing something this
implementation has no concept of.

**4. No persistence across process restarts.** `FLOWMESH_EVENT_TRANSPORT=memory`
means events live in one process. The factories log a WARN on every fallback, and
`_assert_safe_defaults` in `backend/main.py` refuses to start in production with a
non-Kafka transport — so this cannot reach a deployment by accident.

**5. No replication.** A broker failure is not modelled at all.

**6. `lag` is computed from in-memory state**, not from a broker's high-water mark.
A real lag number comes from `highwater - position`. The *shape* is the same; the
value under real broker load is not comparable.

## Why the difference is visible

Two mechanisms, and both exist because a silent difference is how these get
mistaken for each other:

- **The transports are named in the metrics.** Every latency observation carries a
  `transport` label (`grpc` / `in_process`). An `auto` fallback logs at WARN and
  the Grafana panels split on that label, so an operator can see the "grpc" latency
  series go flat because the calls stopped being gRPC — rather than inferring it
  from a suspiciously round number.
- **The reports name the topology.** `docs/load-test-report.md` opens with a table
  of what was substituted, and states plainly which numbers survive the
  substitution. Correctness results do (same handlers, same SQL, same
  constraints); throughput and latency are real *for that topology*.

## The consequence nobody expects

The single biggest difference is the connection model, and it caught a
misleading test:

`sqlite:///:memory:` with `StaticPool` hands **every session the same
connection**. So 500 "concurrent" reservations queue behind one connection and
interleave inside a single transaction. The oversell test passed with 182 of 500
reservations granted and the row ending at `available = -172` — and it was testing
nothing about the conditional `UPDATE` at all.

With a file-backed database and a real pool (`tests/conftest.py::concurrent_factory`),
each session gets its own connection, SQLite serialises the writes the way a row
lock does on PostgreSQL, and exactly 10 of 500 succeed.

This is not a flaw in the in-process log — it is a flaw in a *test fixture* that
looked like a fidelity. Worth stating as a general rule: **a test double's
fidelity is only as good as the part of the system it is standing in for, and the
environment is part of that.**

## When to reach for the real thing instead

Integration tests exist for the properties the in-process implementations
deliberately do not model:

- real PostgreSQL: row-lock semantics, `timestamptz` round-tripping, migrations
- real Kafka: partition assignment under rebalancing, offset commits, `acks="all"`
- real RabbitMQ: publisher confirms, dead-letter exchange routing, prefetch
- real gRPC: a real socket on an ephemeral port (`port=0`), status codes over the
  wire, deadline enforcement

They are marked `integration` and skip themselves when the service is unreachable,
so a bare `pytest` never fails because a container is down. See
`tests/integration/README.md`.