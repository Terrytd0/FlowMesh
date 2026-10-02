# CLAUDE.md

Orientation for an AI assistant (or a new contributor) working in this repository.
The README explains what the system does; this explains how the code is arranged and
which rules are load-bearing rather than incidental.

Read `docs/architecture.md` and `docs/adr/README.md` before changing anything
structural. The ADRs record rejected alternatives, so you do not have to re-derive
them — and re-proposing one is how they get undone.

## The one rule

**Every measured claim must be reproducible by a script that can fail.**

Not approximately reproducible, not reproducible on a good run. `make loadtest`,
`make chaos` and `make reconcile` exit non-zero when a budget is breached, and
their outputs are committed to `docs/`. If you change the pipeline, change a
threshold in `backend/scripts/loadtest.py::Budget` rather than editing a committed
report.

## Layout, and the direction dependencies point

```
  api/  ──▶  pipeline/  ──▶  inventory/  ──▶  database/
                                 │
       ┌─────────────────────────┼─────────────────────────┐
       │                         │                         │
   events/                   queues/                    fraud/
   (EventBus)              (TaskQueue)           (no transport imports)
       │                         │                         │
       └─────────────────────────┴─────────────────────────┘
                                 │
                          observability/
                          (no application imports)
```

Dependencies point inward, one way. `observability/metrics.py` imports **no**
application module — it is the module everything depends on, so an import edge back
into `backend/api/` would make the metrics package unimportable from a script.
`fraud/` imports no generated protobuf and no transport, which is what lets the
model be tested with no socket.

If you want to reach sideways, you are almost certainly moving the wrong module.

## The load-bearing invariants

These are not style. Breaking one produces a bug the suite may or may not catch,
and several of them did.

**1. `Base.metadata` must be populated before `create_all()`.**
`backend/database/session.py` imports `backend.database.models` at module scope for
exactly this reason. Without it `create_all()` is a **silent no-op** — it succeeds,
creates no tables, and the first query fails with `no such table: orders`. Importing
`backend.database.base` alone is not enough; the models module is what registers the
tables. `create_all()` now raises if the metadata is empty.

**2. The engine is keyed by event loop.**
`get_engine()` caches by `asyncio.get_running_loop()`. This process runs two
long-lived loops (FastAPI's, and a consumer's), and a process-wide engine eventually
hands a connection created on one loop to a caller on the other.

**3. Reservation is one conditional `UPDATE`, never read-then-write.**
`backend/database/repositories/inventory.py::reserve_units`. The check and the
decrement must be the same statement or the database cannot hold the row lock
across both. `release_units` and `commit_units` carry the symmetric guards
(`reserved >= qty`) for the same reason.

**4. Nothing slow inside a reservation transaction.**
On PostgreSQL the row lock is held to *end of transaction*. A gRPC call or an HTTP
call inside `reserve_for_order` serialises every concurrent reservation for that SKU.
Events are published **after** the commit, not inside.

**5. `mark_processed()` is an INSERT, in the caller's transaction.**
Not a `SELECT` to ask whether the event was seen; not a flag set after the write.
Both have a window between check and effect, and at-least-once delivery hits that
window constantly. The unique constraint on `event_id` is the gate.

**6. A handler must do nothing at all on a duplicate delivery.**
Not retry, not re-apply, not log an error. `mark_processed` returning `False` means
another delivery already applied it.

**7. Each handler also guards its own *effect*.**
The ledger stops the same event being applied twice; it does not stop two different
events describing the same transition. `inventory.released` for an already-released
reservation returns `False` and changes nothing.

**8. The rolling fraud window is pinned per scoring call.**
`CustomerWindow.scoring_context(customer_id)` … `release_context()` in a `finally`.
The derived features read the pinned customer. A leaked pin is a cross-customer
data leak that produces plausible numbers and no error.

**9. An order is recorded into the window *after* the decision.**
Recording first counts an order toward its own velocity, inflating every score by
exactly one and making the feature look like noise.

**10. The degraded model is refused twice over.**
`backend/pipeline/routing.py::route_for` never auto-approves a `degraded` decision,
regardless of band or score. `HeuristicFallback` is a real (weaker) scorer rather
than a zero vector. Two layers, because one is a single point of failure during an
incident.

**11. `stock_reservations.lines` is snapshotted at hold time.**
The release path returns units from this column, not a join to `order_items`.
Reading the lines from another table means a reservation whose order rows are not
(yet) visible releases nothing while reporting success — the order rows are written
by a different transaction. Found by
`tests/unit/inventory/test_inventory.py::test_the_sweeper_reclaims_an_expired_hold`.

**12. `UtcDateTime` is not `DateTime(timezone=True)`.**
`DateTime(timezone=True)` is a request to the ORM, not a guarantee from the storage
engine. SQLite has no timezone concept and returns **naive** datetimes, so the same
query returns two different types depending on the backend — and the first thing
that breaks is `expires_at < utcnow()`, which is what the expiry sweeper does.
`backend/database/base.py::UtcDateTime` normalises on both the way in and the way out.

**13. Core `UPDATE`s need `populate_existing` on the read that follows.**
The ORM does not know about a Core `UPDATE`, so a row already in the session's
identity map keeps its pre-update values. Without the flag a caller reads stale
stock, and the warehouse-picking routine sends an order to a warehouse that was
emptied a moment ago.

**14. Metric labels are positional and go through `.labels(...)`.**
`Histogram.observe()` takes no label arguments. `metric.observe(x, transport="grpc")`
raises, and a guard that swallowed it would leave the p95 histogram permanently
empty — no exception, no panel, every budget assertion measuring nothing.
`FlowMeshMetrics._guard` re-raises `TypeError` and `ValueError` for exactly this
reason, and `instrumentation_failures_total` is asserted to be zero.

**15. `timer()` is `@asynccontextmanager`.**
A plain `@contextmanager` yields an object with `__enter__` and no `__aenter__`, so
`async with self._metrics.timer(...)` fails at the call site. This happened in two
places and mypy caught both.

**16. An unreadable log is a reconciliation failure, not an empty one.**
`backend/scripts/reconcile.py`. Comparing against an empty set and reporting
`missing: 0` proves nothing while passing; that is the failure mode of every
reconciliation tool that reports a count. Reconciliation is by **event identity**,
never by count — 400 against 400 is also what a pipeline that applied two events
twice and lost two others reports.

**17. `mark_processed` returning `False` is ambiguous, and the `effect` column is
the only thing that resolves it.**
`backend/pipeline/order_processor.py::handle`. `False` means either "a previous
delivery finished this" or "a previous delivery claimed it and then died between
phase 1 and phase 3". Reading both as *finished* stranded orders permanently:
ledgered, present in the database, never scored, never approved, never reviewed,
and no number of redeliveries would change it. So the claim is written
`_CLAIM_IN_PROGRESS` and completed to `_CLAIM_APPLIED` in phase 3's transaction,
and a duplicate delivery resumes only while it still reads in-progress. A *completed*
duplicate must still do nothing at all — that is invariant 6, and the resume path is
not permitted to weaken it.

**18. A cancellation must not tear a database statement in half.**
`backend/events/memory_log.py::_dispatch_one`. `Task.cancel()` delivers a
`CancelledError` at the handler's next await; if that await is a SQL statement the
cancellation does not stop the statement, because `aiosqlite` runs it on a worker
thread that keeps going. The coroutine unwinds while the statement executes, the
rollback is awaited from an already-cancelled context and never completes, and the
connection is never returned to the pool with its transaction closed — it keeps
holding the store's write lock. The replacement consumer then dies on its first
statement. So `cancel()` honours the cancellation *between* records: the in-flight
handler runs to completion, and only then does the consumer stop. `_run` must also
await its partition loops on the way out, because `gather` does not.

**19. `apply_sqlite_pragmas` is a plain `def`, and it must stay one.**
`backend/database/session.py`. It was `async def` and called without `await`, so its
body never ran and *no* pragma was ever applied — no WAL, no `foreign_keys=ON`, no
`busy_timeout` — while its own docstring described a fix that was not in effect. The
tell was in `backend/loadtest/harness.py`, which had grown its own working copy:
when the load test needed a pragma the application engine did not have, the copy is
where it went. One helper, used by both.

**20. SQLite write transactions must `BEGIN IMMEDIATE`.**
Not a pragma but the same lesson. A DEFERRED transaction takes a SHARED lock on its
first read and needs to upgrade to RESERVED to write; SQLite cannot wait out that
upgrade without risking a deadlock against itself, so it returns SQLITE_BUSY
*immediately* and never consults the busy handler. `busy_timeout` is therefore
useless for the read-then-write transactions this pipeline actually issues. Taking
the write lock up front is what turns a conflict into a wait.

**21. A route handler, and everything its annotations name, must be a module
global.**
`backend/main.py`. The module has `from __future__ import annotations`, so handler
annotations are strings that FastAPI resolves against the module's globals — and on
a `NameError` it silently demotes the parameter to a *query* parameter instead of
raising. `POST /auth/login` was defined inside a factory along with its
`Depends(get_auth)` target, so every request got a 422 mentioning `["query","body"]`
and nobody could log in — on the first command in `README.md`'s quick start. It
survived because no test called that endpoint: every authorisation test mints its own
token. `tests/unit/test_route_annotations.py` now checks every route, and
`test_the_router_tree_walk_actually_finds_the_endpoints` checks the checker, because a
walk that sees only `/docs` would pass by asserting nothing.

**22. Backlog is not data loss.**
`backend/scripts/loadtest.py::Budget`. When the consumer does not drain, the orders
it never reached are absent from the database — but their events are still in the log
with uncommitted offsets, so they are *backlog*. The previously committed report got
this wrong and shipped it: "635 published orders had no persisted effect (data
loss)" about a run that had lost nothing. On an unfinished drain the data-loss budget
is **undetermined**, not breached and not passed.

## Commands

```bash
make check              # format-check + lint + typecheck + test  (what CI runs)
make test               # pytest; integration tests skip themselves
make test-integration   # needs postgres/kafka/rabbitmq, see tests/integration/README.md
make loadtest           # 500 orders/sec, writes docs/load-test-report.md
make chaos              # consumer kill + replay, writes docs/chaos-test.md
make reconcile          # log vs database, writes docs/reconciliation.md
make evidence           # all three of the above
make all-local          # the whole pipeline in one process (SQLite, in-process transports)
make proto              # regenerate gRPC stubs
make up / make down     # the full Docker Compose stack
```

All three evidence scripts exit non-zero on a breach, and all three currently
**pass**. `make reconcile` accepts `LOG_FILE=events.json` to reconcile a broker
export instead of generating its own log.

`make` is Unix-only. On plain Windows PowerShell use the venv directly:
`.venv\Scripts\python -m ruff check .`, `-m mypy .`, `-m pytest`.

CI runs `make check`, the container job, and a **short load test** whose latency
and correctness budgets must pass. The full 500/sec run and the chaos test are
excluded on purpose: the chaos test takes ~5s of wall clock and the load test is
timing-sensitive, and a red build from machine jitter trains people to re-run it.

## Testing rules

**Rule 1 — a bare `pytest` with no flags and no services running must pass, and
must be safe to run.** Never make a test fail because Postgres is down.

**Rule 2 — if a test overrides a dependency, some other test has to run the real
one.** This is why `authenticated_client` exists alongside `api_client`: the suite
default disables auth, so an authorisation test against `api_client` passes
vacuously. It also caught a hardcoded `"CUST-1"` in the ownership check that let any
customer read any seeded customer's orders.

**Rule 3 — no test module basename is reused across directories.** `tests/` has no
`__init__.py`, so pytest's rootdir-based naming collides.

**Rule 4 — the suite is hermetic.** `tests/conftest.py` pins every transport to its
in-process implementation.

**Rule 5 — `TestClient` must be used as a context manager.** `TestClient(app)` alone
does not run the lifespan, so `app.state` stays empty and the first route raises
`AttributeError: 'State' object has no attribute 'rate_limiter'`.

**Rule 6 — a test that measures the system must be able to report red.** The load
and chaos harnesses assert their own budgets; the metrics are read back out of the
registry (`value_of`), not from a mock's call list.

**Rule 7 — prefer a real connection for concurrency tests.** `sqlite:///:memory:`
with `StaticPool` hands every session the same connection and cannot express
concurrency at all. Use `concurrent_factory`.

## When you add a feature

- **A new event type** — add it to `EventType`, write its payload model, add it to
  the consumer's dispatch table, and decide explicitly whether it belongs in
  `EXPECTED_UNLEDGERED` in `backend/scripts/reconcile.py`. That last one is the trap:
  an event type that is legitimately never ledgered must be named *with a reason*,
  or reconciliation will report it as data loss forever.
- **A new topic** — add it to `Topic`, create it in both bus implementations'
  `ensure_topics`, and pick a partition key deliberately. `crc32(key)`, never
  `hash()`, or two services will disagree on which partition an order's events
  belong to.
- **A new queue** — declare it with a dead-letter exchange in
  `backend/queues/rabbitmq_queue.py`, and mirror the depth reporting in
  `InMemoryTaskQueue` using `_dead_letter_key` (one function; the writer and the
  two readers once used three different keys and the gauge read zero during an
  incident).
- **A new table** — add the model, then a migration. `alembic check` runs in CI and
  fails on drift. `KNOWN_TABLES` in `backend/database/session.py` is the assertion
  that the model registered.
- **A new metric** — declare it in `FlowMeshMetrics` with a **fixed** label set.
  Never a label derived from user input. Prefer a counter plus a derived rate over a
  gauge you have to keep updated.
- **A new fraud feature** — add it to `FEATURE_NAMES`, give it a weight in
  `data/model/fraud_weights.json` (a missing one is a hard error at load), and give
  it a label in `REASON_LABELS`. A feature with no label is a column an operator
  learns to ignore.
- **A new threshold** — add it to `Settings` and to the relevant `Budget`, and make
  the band/routing functions assert consistency between them rather than trusting
  the caller to have used the same ones.

## Things that will look wrong but are not

- **`InMemoryEventLog` is not a mock.** It has real offsets, real consumer groups,
  real redelivery and real replay. See
  [ADR-005](docs/adr/005-in-memory-log-not-a-mock.md).
- **`FraudScoringEngine.window` is bounded at 200k customers** and evicts by
  recency. An unbounded map is how a long-running scorer becomes the incident.
- **`backend/grpc_service/generated/` is committed and excluded from ruff/mypy.**
  Change the `.proto` and run `make proto`. Nothing warns you if it goes stale; the
  suite fails on an import error instead.
- **The load test generates duplicate SKUs on purpose.** Real baskets repeat them,
  and `order_items` enforces `UNIQUE (order_id, sku)`. The API merges them at the
  boundary (`OrderCreateRequest.merged_items`); the generator mirrors that.
- **Latency figures in the reports are bucket upper bounds**, stated as such. They
  are read from the histogram Prometheus scrapes so the report and the dashboard
  cannot disagree.
- **stdout logging is on by default.** JSON logging is opt-in
  (`FLOWMESH_LOG_LEVEL_JSON`), because a container log someone actually reads beats
  one that is easier to query.

## Known gaps

Recorded rather than hidden; each has a test that fails when it is fixed.

1. `POST /orders` does not bind `customer_id` to the token —
   `test_an_order_can_be_placed_in_another_customers_name`.
2. The rolling fraud window is per-process, so with N scoring replicas each sees
   1/N of a customer's velocity.
3. `make reconcile` cannot read a *live* Kafka broker — it reconciles an export
   (`--log-file`, in `kcat -J` shape) or a log it generates from its own in-process
   run. Reconciling a broker directly would mean a Kafka client here, and the
   export is the same artefact one line of `kcat` produces.
4. No refresh tokens, and the user store is a seeded fixture.
5. The consumer tops out at ~35 orders/sec on one process against one SQLite file.
   Measured, not assumed: ~17 statements per order, each a round trip to an
   `aiosqlite` worker thread. fsync is *not* the constraint — 2.28 ms/commit at
   `synchronous=FULL` against 0.83 ms at `synchronous=OFF`, for a ~25 ms per-order
   cost. Closing the gap to 500/sec needs Postgres with real pooling and several
   consumer replicas, which is the deployment the architecture targets.

## Where the interesting arguments are

- [ADR-004](docs/adr/004-kafka-vs-rabbitmq.md) — why two brokers, and the failure
  each one handles that the other does not
- [ADR-005](docs/adr/005-in-memory-log-not-a-mock.md) — why the in-process log
  exists, and what it deliberately does not model
- [ADR-006](docs/adr/006-llm-on-the-hot-path.md) — why an LLM cannot meet a 200ms
  p95, and what it is for instead
- [ADR-007](docs/adr/007-grpc-status-codes.md) — the status-code mapping, and two
  ways the boundary was wrong first
- [architecture.md](docs/architecture.md) — the flow, the failure modes, and the
  limits of the load and chaos harnesses