# FlowMesh architecture

The event flow, the failure modes at each boundary, and the limits of what the
evidence scripts actually prove. `CLAUDE.md` has the code-level invariants; this
has the shape of the system.

## The flow, end to end

```
1.  POST /orders
      rate limit (Redis, fixed window, shared across replicas)
      idempotency: cache lookup → DB lookup → unique-constraint fallback
      persist order + order_items + first audit row      [transaction 1]
      publish order.accepted                            ← after commit

2.  OrderProcessor consumes order-events            [consumer group, offset committed after handler]
      claim event_id in processed_events                 [transaction 2]
      persist order + audit "accepted"
                                                          ← commit
      build OrderContext
      ScoreOrder over gRPC                                ← OUTSIDE any transaction
                                                          [transaction 3, if degraded]
      persist fraud_scores + order.fraud_score + audit "scored"
      publish order.scored                              ← after commit
      route: low → reserve stock | else → publish review task

3.  Reservation (inside transaction 3's sibling, before the route)
      conditional UPDATE per line, all-or-nothing across the order
      insert stock_reservations with the lines snapshotted + TTL
      publish inventory.reserved                       ← after commit

4.  ReviewWorker consumes review-queue                [ack after handler returns]
      decide_review (idempotent; AlreadyDecided on redelivery)
      commit or release the reservation                  [one transaction]
      order status + audit + notification outbox row
      publish order.approved | order.rejected          ← after commit

5.  InventoryWorker consumes inventory-events
      confirm/repair the reservation row
      audit the transition
```

Numbering the transactions matters: **the gRPC scoring call is outside every
database transaction**, and that is not a style preference. On PostgreSQL a row lock
is held to end-of-transaction, so a 200ms scoring call inside a transaction would
serialise every concurrent order touching the same rows behind it.

## Failure modes, boundary by boundary

### API → event log

**The order is durable before the event is published.** The reverse would make the
event visible to a consumer that cannot yet read the order.

The cost of this ordering is a real window: if the process dies between the commit
and the publish, an order exists that no consumer will ever see. Three things
address it rather than pretending it away:

1. the client's `Idempotency-Key` means a retry re-runs the whole path and repairs it
2. `POST /orders` returns 503 rather than 202 when the publish fails, and audits
   `publish_failed`
3. `make reconcile` reports orders on the log with no database row — and reports
   that it *could not check* if the log is unreadable, rather than reporting a
   vacuous pass

### Event log → consumer

**Delivery is at-least-once; the effect is applied once.** Two mechanisms, and both
are needed:

- `processed_events.event_id` as a primary key, INSERTed in the same transaction as
  the effect — this stops the same event being applied twice
- each handler guards its own *effect* independently (`release_reservation` returns
  `False` for an already-released hold) — this stops two *different* events
  describing the same transition from both applying it

**A refused claim is ambiguous, and the `effect` column resolves it.** The order
processor writes the ledger row in phase 1 and the decision in phase 3, with the score
computed in between, outside any transaction. A delivery that dies in that gap leaves
a claim behind that is indistinguishable from a completed one — so reading `False` as
"already done" stranded the order permanently: ledgered, present, never scored, never
reviewed, nothing to retry it. The claim therefore records `accepted` on the way in and
is completed to `scored` in phase 3's transaction, and a redelivery resumes only while
it still reads in-progress. A *completed* duplicate still does nothing at all.

A cancellation also has to be handled deliberately. `Task.cancel()` at a SQL await does
not stop the statement — `aiosqlite` runs it on a thread that keeps going — so the
transaction would never be rolled back and the connection would keep holding the write
lock against the replacement consumer. The in-process log therefore lets an in-flight
handler finish and honours the cancellation between records.

**A handler that cannot process a record stops the subscription** rather than
committing past it. Committing past is silent data loss: the offset advances, the
record is never handled again, and nothing reports an error. The retry bound is 3
attempts with backoff, then `HandlerFailedError`.

### Consumer → scorer (gRPC)

See [ADR-007](adr/007-grpc-status-codes.md). The load-bearing points: `UNAVAILABLE`
for an unloaded model so a client fails over rather than dropping an order; the
`auto` transport falls back and says so at WARN; and every decision is labelled with
the transport that produced it.

### Scorer unavailable

Two layers, deliberately:

1. `FraudScoringEngine` loads `HeuristicFallback` rather than refusing to start —
   a scorer that will not boot stops all order processing
2. `route_for()` refuses to auto-approve **any** `degraded` decision, whatever its
   band or score

One layer would be a single point of failure during an incident. The cost of two is
that a model outage fills the review queue — which is a cost you can see, and the
right thing to see.

### Scorer failure on one order

The order is **held**, with the reason recorded, and the event is committed.
Retrying forever would stall the partition and stop every order behind it; dropping
it would lose a customer's order. Holding puts it on a dashboard.

### Reservation

**A reservation is one conditional `UPDATE` per line.** Read-then-write oversells by
the width of the race, reliably rather than occasionally, because at 500 orders/sec
the race is wide.

**All-or-nothing across the order's lines.** Partial reservation is worse than none:
the customer is told two of four items are coming and the other two are silently
unavailable with no order to explain why.

**The lines are snapshotted onto the reservation row.** The release path reads them
from there, not from `order_items` — see invariant 11 in `CLAUDE.md` for why.

**A hold expires.** Without a TTL, an order held for review holds its stock forever
and the catalogue quietly undersells. The sweeper runs on an interval in the
inventory worker's process.

### Review decision

**A decision is applied exactly once, and the ack follows it.** Crash before the
commit → redelivered → proceeds. Crash after the commit, before the ack →
redelivered → `AlreadyDecided` → acked and moved on. The second case is what the
check constraint on `review_queue_items` also prevents.

**The API does not apply the decision.** It records that a decision was *requested*
and publishes a task; the worker applies it and performs the inventory transition in
the same transaction. An API route that both decided and applied would be a second
implementation of that transition.

**The notification is an outbox row**, written in the decision's transaction and
delivered by a separate loop. A crash between "approved" and "customer told" leaves
a row with `sent_at IS NULL` — a visible gap — rather than a customer who was never
notified and no record that they should have been.

## Consistency model

**Per-key ordering, not global ordering.** All of one order's events land on one
partition. Records with different keys interleave. Any assumption of a total order
across orders is wrong.

**The ledger gives effectively-once effects, not exactly-once delivery.** At-least-once
delivery plus an idempotent effect. The two windows, both closed:

| crash point | what happens |
| --- | --- |
| before the transaction commits | offset uncommitted → redelivered → rolled-back transaction left no trace |
| after commit, before the offset commit | redelivered → `mark_processed` finds the row → skip |
| between the processor's two transactions | claim reads in-progress → redelivery **resumes** rather than skipping |

**Stock is strongly consistent; reads are not.** `available` is stored rather than
computed as `on_hand - reserved`, because that subtraction under concurrency is a
rounding error waiting to become an oversell. The check constraint
`available = on_hand - reserved` is the backstop.

**The fraud window is eventually consistent, and per-process.** With N scoring
replicas each sees 1/N of a customer's velocity. Stated in
`backend/fraud/profile_window.py`; the fix is a shared window store.

## SQLite, and where the two dialects genuinely differ

One place, and it is load-bearing: the SQLite path is what lets the load test, the
chaos test, the reconciliation and the whole unit suite run the **real** SQL —
including the conditional `UPDATE` that prevents oversell — with no server running.
Same models, same statements, two dialects.

Four settings, applied by one function (`backend/database/session.py::apply_sqlite_pragmas`)
that both the application engine and the load-test harness use. They were previously
*not applied at all* — the function was `async def` and called without `await`, so its
body never ran, while its docstring described a fix that was not in effect. The tell
was in the harness, which had grown its own working copy.

| setting | why |
| --- | --- |
| `journal_mode=WAL` | without it every write blocks every reader, and the load test measures SQLite's locking rather than the pipeline |
| `foreign_keys=ON` | **off by default in SQLite**, so `order_items -> orders` was unenforced on every SQLite-backed run while being enforced on PostgreSQL. A constraint that only exists in production is not a constraint |
| `busy_timeout` | SQLite admits one writer; without a timeout a contended write fails instantly instead of waiting |
| `synchronous=NORMAL` | the WAL-recommended durability/crash-safety trade, *not* a speed knob — see below |

Plus `BEGIN IMMEDIATE`, which is the non-obvious one. A DEFERRED transaction takes a
SHARED lock on its first read and needs to upgrade to RESERVED to write. SQLite cannot
wait out that upgrade without risking a deadlock against itself — two readers each
holding SHARED, each waiting for the other — so it returns SQLITE_BUSY **immediately**
and never consults the busy handler. `busy_timeout` is therefore useless for the
read-then-write transactions this pipeline actually issues. Taking the write lock up
front is what turns a conflict into a wait; it is what stopped the chaos test's
restarted consumer dying on its first statement.

What SQLite does *not* model, and must not be asked to: multi-writer concurrency. The
oversell invariant is a database claim and only PostgreSQL can be asked properly —
see `tests/integration/README.md`.

## Observability

One registry per process, one `FlowMeshMetrics` wrapping every metric, injectable
for tests.

**Buckets are chosen around the 200ms budget.** Prometheus's defaults put eight of
ten lines between 5ms and 100ms and cannot resolve 200ms at all. A test asserts the
configured edges bracket the budget.

**No label is derived from user input.** A per-order-id label is an unbounded
time-series store and an attack on the monitoring system. The label sets are fixed
and declared in one place.

**`instrumentation_failures_total` is asserted to be zero.** `_guard` swallows
runtime instrumentation errors so a metric problem cannot fail an order — correct,
and a terrible way to lose a metric family silently. The counter is what makes that
safe, because the wrong way to call `observe()` raises `TypeError` and would
otherwise leave the p95 histogram permanently empty while every budget assertion
measured nothing.

**`/metrics` touches no dependency.** A scrape endpoint that can block on a failing
database is one that stops answering exactly when it is needed, and a scrape timeout
looks identical to a dead service.

**`/healthz` and `/readyz` differ.** Liveness touches nothing (a liveness probe that
fails when Postgres restarts gets the API killed, and a killed API does not
reconnect). Readiness checks the database and returns 503, which removes the
instance from the load balancer without killing it.

## What the evidence scripts do and do not prove

### `make loadtest` — 500 orders/sec

**Proves**: zero oversells and zero data loss over 5,001 orders, a scoring p95 inside
the 200ms budget, and that the consumer clears its own backlog at a sustained rate.

**Does not prove** 500 orders/sec end to end. Three rates are reported separately for
exactly that reason: the producer ingests at 500/sec, the consumer sustains ~35/sec
while catching up, and end to end the run is ~35/sec. The consumer is the bottleneck
and the report says so.

The constraint was **measured, not assumed**: ~17 SQL statements per order, each a
round trip from the event loop to an `aiosqlite` worker thread. Benchmarking the same
engine directly showed 2.28 ms/commit at `synchronous=FULL` against 0.83 ms at
`synchronous=OFF`, against a per-order cost of ~25 ms — so fsync is not the
constraint, and `synchronous=NORMAL` is chosen for crash-safety rather than speed.

`Budget.min_end_to_end_per_sec` is 0 and asserts nothing, because a budget nobody can
meet is worse than no budget. What *is* asserted is the producer rate,
`min_drain_per_sec` (the one field that says "the pipeline got slower"),
`max_scoring_p95_ms`, `max_oversells` and `max_unaccounted_orders`.

Two things the harness had wrong, both now fixed and both recorded in
`backend/scripts/loadtest.py`:

- the drain deadline was 120s against a consumer needing ~157s for 5,001 orders, so
  every run failed and the committed report said `FAIL` for a reason that had nothing
  to do with the pipeline;
- an undrained backlog was reported as **data loss** ("635 published orders had no
  persisted effect"), which invented 635 losses on a run that had lost none. The
  events were still in the log with uncommitted offsets. On an unfinished drain the
  data-loss budget is now *undetermined* — neither breached nor passed.

**Real**: the handlers, the SQL, the idempotency ledger, the reservation logic, the
band classification, the scoring. Correctness results — zero oversells, zero data
loss — are real.

**For that topology only**: throughput and latency. The run substitutes four
transports (in-process log, in-process queue, in-process scorer, SQLite) because one
machine cannot host a broker and be the load generator. The report opens with a
table saying so.

**The open-loop generator matters.** Each producer sleeps until its next scheduled
slot and publishes regardless of whether the consumer kept up. A closed-loop
generator self-throttles to the slowest component, so it would report its own input
rate as throughput and quietly measure nothing. The catch-up time after generation
stops is the backpressure signal, and it is reported separately — including it in
throughput would conflate two different numbers.

### `make chaos` — kill a consumer mid-stream

**Proves**: reconciliation by event identity (the missing ids are *named*); the kill
landed with a real backlog behind it; and the uncommitted tail was delivered after
the restart.

**Does not prove**: broker-side rebalancing. The kill is a real `Task.cancel()` on
the consumer's own task — the same path a SIGTERM takes — but it is the in-process
log, whose group offsets are a position in history. Real Kafka additionally
rebalances the partition to another member, and that timing is not exercised.

Three versions of this test were wrong before the current one, and each failure is
recorded in `backend/scripts/chaos_test.py`:

- counting *double* deliveries to prove the replay — which is zero by design, since
  the uncommitted tail was never delivered the first time
- recording deliveries from every event type, which counted the processor's own
  `order.scored` output and reported 392 replays against a backlog of 195
- recording deliveries by assigning to `processor.handle` **after** `build_pipeline`
  had already started the consumer. `OrderProcessor.subscription()` captures
  `self.handle` as a bound method when it builds the `Subscription`, so the
  assignment replaced an attribute nothing dispatched through any more. The recorded
  list stayed empty, `replayed` was 0, and the test failed on its own third
  assertion — reporting a replay failure having measured no deliveries at all, and
  would have done so forever.

The assertion that catches all three ("no event was delivered after the restart that
had not been delivered before it") is the one that measures the actual claim.

**What the kill cost to get right.** `cancel()` returning does not mean the
subscription is stopped: a handler suspended inside a SQL statement survives it, and
its transaction is never rolled back, so the connection keeps holding SQLite's write
lock. The restarted consumer then died on its first statement with
`database is locked` — three retries inside `dispatch_with_retries` burned in ~150ms,
`HandlerFailedError` took the subscription down, and because `subscribe` never awaits
the task the failure was invisible. The chaos test sat in `wait_for_drain` for its
full 60s timeout and then died of an unrelated `PermissionError` cleaning up the
database file, which says nothing about the consumer dying. Two fixes: the in-process
log lets an in-flight handler finish before honouring the cancellation, and
`_await_replay_or_report_death` watches the task so a dead consumer is named in
milliseconds instead of after a timeout.

### `make reconcile` — log against database

**Proves**: the two agree, by identity, and the database-side invariants hold.

**Cannot read a live Kafka broker**, because a broker is not something a script
reaches into. It has two honest routes instead:

- by default it *generates* the log: the same processors, SQL and ledger as the load
  test run over a short stream, drained to completion, then read back out of
  `published_envelopes()` and reconciled against **that same run's** database;
- `--log-file` takes an export instead, in `kcat -J` shape (NDJSON, base64 `payload`),
  a JSON array, or one envelope per line.

If neither is readable it says so and exits non-zero. That is the whole point of the
design: a script that compares against an empty set reports `missing: 0` and passes,
having verified nothing. A database that cannot be reached is reported the same way,
rather than as a traceback — a crash writes no report, and the run that most needs the
document is the one where nothing is listening.

`EXPECTED_UNLEDGERED` names the event types legitimately absent from the ledger,
each with a reason. A suppression without a reason is a suppression, and a
suppression is how a reconciliation report starts reporting success while a bug is
live.

## Security posture

| | |
| --- | --- |
| Auth | JWT (HS256, Argon2 hashes), issuer verified |
| Roles | `customer` / `supervisor` / `admin`; only a supervisor may decide a review |
| Order ownership | 404 for both "no such order" and "not yours" — a 403 is an existence oracle |
| Idempotency | unique constraint on `orders.idempotency_key` — the authority, not the Redis cache |
| Rate limiting | fixed window in Redis, so it is shared across replicas |
| No card data | `PaymentDetails` takes a BIN and last four; a full PAN would make the log a PCI scope |
| No model internals to customers | a held order returns "order held for review", never the reason |
| Startup refusals | production mode refuses auth-disabled, the default JWT secret, and non-Kafka transports |

**Known gap**: `POST /orders` does not bind `customer_id` to the token. Recorded in
`CLAUDE.md` and asserted by
`tests/integration/test_orders_api.py::test_an_order_can_be_placed_in_another_customers_name`
so it stays visible rather than being mistaken for a missing check.

**A route that cannot be reached is an outage, not a 422.** `POST /auth/login` was
defined inside a factory function along with its `Depends(get_auth)` target, in a
module using `from __future__ import annotations`. FastAPI resolves handler
annotations against the module's globals, and on a `NameError` it silently demotes the
parameter to a *query* parameter rather than raising. Every login attempt therefore
answered 422 mentioning `["query", "body"]` and nobody could log in — on the first
command in the quick start. Nothing raised and nothing failed to import; the OpenAPI
schema quietly documented query parameters.

It survived review because **no test called that endpoint**: every authorisation test
mints its own token with `create_access_token`, so the one route that turns a password
into a token had no coverage at all. `tests/integration/test_auth_api.py` covers it,
and `tests/unit/test_route_annotations.py` checks every route's annotations resolve —
including a test that checks the checker, since a walk that sees only `/docs` passes by
asserting nothing.

## Deployment

Compose services: `kafka` (KRaft, no ZooKeeper), `rabbitmq`, `postgres`, `redis`,
`api`, `fraud`, `orders`, `inventory`, `review`, `prometheus`, `grafana`.

Separate processes for the consumers, for three reasons that are about failure
isolation rather than taste:

- a consumer crash loop leaves customers able to place orders
- scoring concurrency and API concurrency are tuned against different limits
- restarting a consumer replays the last few seconds' backlog, which should not
  happen on every deploy

`FLOWMESH_EVENT_TRANSPORT=auto` falls back to the in-process log with a WARN;
production mode refuses to start with anything but `kafka`. The fallback cannot
reach a deployment by accident.