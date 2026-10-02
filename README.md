# FlowMesh — real-time order & fraud pipeline

Event-driven order pipeline for a 12-warehouse retailer: **Kafka** for the event
log, **RabbitMQ** for the task queue, **gRPC** for the scoring boundary,
**Prometheus/Grafana** for the proof.

Every measured claim in this repository has a script that can fail:

```bash
make loadtest    # 500 orders/sec, p95 budget, zero oversells  -> docs/load-test-report.md
make chaos       # kill a consumer mid-stream, prove zero loss -> docs/chaos-test.md
make reconcile   # log vs database, by identity                  -> docs/reconciliation.md
make check       # format + lint + types + tests
```

---

## What this demonstrates

| | |
| --- | --- |
| **Event-driven pipeline** | partitioned log, consumer groups, committed offsets, idempotent handlers, replay after consumer death |
| **Kafka *and* RabbitMQ** | and why running both is correct rather than redundant ([ADR-004](docs/adr/004-kafka-vs-rabbitmq.md)) |
| **Online inference** | streaming features + a legible logistic model; an LLM spent only where the model is *unsure* ([ADR-006](docs/adr/006-llm-on-the-hot-path.md)) |
| **Concurrency done properly** | a conditional `UPDATE` that cannot oversell, proven with 500 racing reservations |
| **Typed service boundary** | protobuf + gRPC, with a status-code mapping that means something ([ADR-007](docs/adr/007-grpc-status-codes.md)) |
| **Observability that can report red** | custom metrics with buckets chosen around the 200ms budget; dashboards that cannot silently disagree with the tests |
| **Evidence over claims** | three scripts, each exiting non-zero on failure, each documenting what it does *not* test |

---

## Architecture

```
                    ┌──────────────┐
  POST /orders ─────▶  FastAPI     │──▶ order-events ──────────┐
  (JWT, Idempotency-Key)  ingress   │                            │
                    └──────────────┘                            ▼
                                            ┌──────────────────────────┐
                                            │  OrderProcessor          │  claims event_id
                                            │  (consumer group)        │  → scores → routes
                                            └────┬────────────────┬────┘
                                    gRPC ScoreOrder │                │ RabbitMQ publish
                                                 ▼                ▼
                                    ┌────────────────────┐  ┌──────────────────┐
                                    │ FraudScoring      │  │ review-queue     │
                                    │ (streaming window)│  │ + DLQ            │
                                    └────────────────────┘  └────────┬─────────┘
                                                                      ▼
                                                            ┌──────────────────┐
                                                            │ ReviewWorker     │
                                                            │ decision → audit │
                                                            │ → notification   │
                                                            └──────────────────┘
                                                 │
                        inventory-events ◀───────┘
                                 │
                    ┌────────────▼─────────────┐        ┌──────────────┐
                    │ InventoryWorker (×12 WH) │───────▶│  PostgreSQL  │
                    │ + expiry sweeper         │        └──────────────┘
                    └──────────────────────────┘
                                                 ▲
             Prometheus /metrics ──▶ Grafana ─────┘
```

**Why two brokers.** Kafka is a *log of what happened* — replayable, partitioned
by key, high-throughput. RabbitMQ is a *queue of what still needs doing* —
acknowledged, dead-lettered, redelivered on nack. `review.requested` belongs to the
second kind: if it is lost a fraudulent order sits unscored and the customer is
charged; if it is replayed three hours later it is worse than useless. The full
argument, including what would have gone wrong with either broker used for both
jobs, is [ADR-004](docs/adr/004-kafka-vs-rabbitmq.md).

---

## Quick start

### The whole pipeline, one process, nothing else running

```bash
uv sync --extra dev
cp .env.example .env

# SQLite + in-process transports: no Kafka, no RabbitMQ, no Postgres.
export FLOWMESH_DATABASE_URL="sqlite+aiosqlite:///./data/runtime/flowmesh.sqlite3"
export FLOWMESH_EVENT_TRANSPORT=memory
export FLOWMESH_QUEUE_TRANSPORT=memory
export FLOWMESH_FRAUD_TRANSPORT=in_process

make seed          # 12 warehouses + stock
make all-local     # API + order processor + inventory worker + review worker
```

`make all-local` is `backend/scripts/dev_stack.py`. It builds the event bus, the task
queue, the session factory and the metrics registry **once** — in the API's lifespan —
and binds every worker to those objects. That sharing is the point: five `run_*`
scripts launched as tasks would each build their own in-process log, so the order
processor would publish into one log and the inventory worker would consume from
another, and the stack would appear to run while processing nothing.

Add `--grpc-fraud` to serve the scoring boundary over a real gRPC socket in the same
process, which exercises the protobuf conversion, the status-code mapping and the
deadline path while the brokers stay substituted.

To measure rather than browse:

```bash
make loadtest      # the real pipeline at 500 orders/sec
make chaos
make reconcile
```

Every transport substitution is declared in `backend/events/factory.py` and
`backend/queues/factory.py`, and every one logs a WARN when `auto` falls back. The
handlers, the SQL, the idempotency ledger and the reservation logic are **identical**
in both modes — only the brokers differ.

### The full stack

```bash
make up            # kafka (KRaft) · rabbitmq · postgres · redis · api · fraud · workers
open http://localhost:8000/docs
open http://localhost:3000          # Grafana, dashboards provisioned automatically
```

Seeded logins (`POST /auth/login`):

| email | role |
| --- | --- |
| `customer@example.com` / `customer-pass` | customer |
| `supervisor@example.com` / `supervisor-pass` | supervisor — can approve/reject |
| `admin@example.com` / `admin-pass` | admin |

```bash
TOKEN=$(curl -s -XPOST localhost:8000/auth/login \
  -H 'content-type: application/json' \
  -d '{"email":"customer@example.com","password":"customer-pass"}' | jq -r .access_token)

curl -XPOST localhost:8000/orders -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: demo-1' -H 'content-type: application/json' -d '{
    "customer_id": "CUST-1",
    "items": [{"sku":"SKU-TSHIRT-M","quantity":2,"unit_price_cents":2400}],
    "payment": {"bin":"411111","last4":"4242","card_country":"US",
                "billing_country":"US","shipping_country":"US","ip_country":"US"}
  }'
```

Retry that same command: the same `Idempotency-Key` returns the same order and
publishes nothing.

---

## Measured results

Produced by the scripts above; the committed reports are the output, not a
screenshot of a good minute.

| claim | where | how it can fail |
| --- | --- | --- |
| 500 orders/sec sustained *ingest* | [load-test-report](docs/load-test-report.md) | throughput budget, `Budget.check` |
| fraud-scoring p95 < 200ms | [load-test-report](docs/load-test-report.md) | histogram bucket bound > budget |
| consumer keeps up with its own backlog | [load-test-report](docs/load-test-report.md) | `Budget.min_drain_per_sec` |
| **zero oversells under concurrency** | [load-test-report](docs/load-test-report.md) | 5,001 racing reservations, exact count |
| **zero data loss** | [load-test-report](docs/load-test-report.md) | every published `order_id` reconciled |
| **zero data loss on consumer death** | [chaos-test](docs/chaos-test.md) | reconciliation by event identity |
| log and database agree | [reconciliation](docs/reconciliation.md) | set comparison, not counts |

**On the throughput numbers.** Three rates, because one number would have hidden the
gap. The producer ingests at 500/sec; the consumer sustains ~35/sec while catching up;
end to end the run is ~35/sec. The single-process run substitutes four transports,
and the report says so in a table. Correctness results from that run are real — same
handlers, same SQL, same constraints — and they are the ones asserted at zero.
Throughput and latency are real *for that topology*, and the measured reason the
consumer is the bottleneck is in the report: ~17 statements per order, each a round
trip to an `aiosqlite` worker thread. Benchmarking ruled out fsync (2.28 ms/commit at
`synchronous=FULL` against 0.83 ms at `synchronous=OFF`). Reaching 500/sec end to end
needs Postgres with real pooling and several consumer replicas — the deployment this
architecture targets, which a laptop cannot measure.

**On the latency numbers.** The p95 figures are *bucket upper bounds*
reconstructed from the same `flowmesh_scoring_latency_seconds` histogram that
Prometheus scrapes, so the report and the dashboard cannot disagree. Prometheus's
default buckets cannot resolve a 200ms p95 at all; the configured ones are chosen
around that budget and a test asserts they bracket it.

---

## The four things worth reading the code for

### 1. Conditional decrement (`backend/database/repositories/inventory.py`)

```sql
UPDATE inventory
   SET available = available - :qty, reserved = reserved + :qty
 WHERE warehouse_id = :wh AND sku = :sku AND available >= :qty
```

`rowcount == 0` is the refusal. The read-then-write version oversells by the width
of the race — reliably, not occasionally, because at 500 orders/sec the race is
wide. Check constraints (`available >= 0`, `available = on_hand - reserved`) back it
up at the database.

### 2. Idempotency (`backend/database/repositories/idempotency.py`)

`mark_processed()` is an `INSERT` on the event-id primary key, **inside the
transaction that applies the effect**. Not a `SELECT` to ask whether the event was
seen, not a flag set afterwards — both have a window, and at-least-once delivery
hits that window constantly. This is what makes the chaos test's "zero loss"
claim testable.

### 3. Two refusal paths for the degraded model (`backend/pipeline/routing.py`)

A model that fails to load does not become a model that approves everything: the
fallback is a real (if weaker) scorer, *and* the routing policy refuses to
auto-approve any decision it made. Two layers, because one would be a single point of
failure during an incident.

### 4. Finishing an interrupted handler (`backend/pipeline/order_processor.py`)

`mark_processed` returning `False` is genuinely ambiguous: a previous delivery either
*finished* this event or *claimed it and died* between phase 1 and phase 3. Reading
both as finished stranded orders permanently — ledgered, present, never scored, never
reviewed, and no number of redeliveries would change it. So the ledger's `effect`
column is the discriminator: written in-progress, completed in the same transaction
as the effect it describes. The chaos test found it, and only because it reconciles
by identity — 400 events, 400 ledger rows, **399 orders**. A count said "one missing";
naming them said which.

---

## Layout

```
backend/
  config/          settings — the only reader of the environment
  core/            clock, ids, logging
  events/          EventBus port + Kafka and in-process implementations
  queues/          TaskQueue port + RabbitMQ and in-process implementations
  fraud/           features, model, bands, LLM reasoner, scoring engine
  grpc_service/    proto conversion, server, clients (grpc | in-process | auto)
  database/        models + repositories — all SQL lives here
  inventory/       reservation policy and lifecycle
  pipeline/        order processor, inventory worker, review worker, routing
  observability/   Prometheus metrics, on one registry
  loadtest/        harness, report rendering, metric snapshots
  api/             routes, schemas, rate limiting
  scripts/         the runnable entry points, incl. the three evidence scripts
                   and dev_stack.py (`make all-local`)
proto/flowmesh/v1/ fraud.proto
docs/adr/          004 Kafka vs RabbitMQ · 005 in-memory transports · 006 LLM
                   · 007 gRPC status codes
docs/              architecture.md + the three generated reports
```

**Dependencies point one way.** `api/` → `pipeline/` → `inventory/` →
`database/`, with `events/`, `queues/`, `fraud/` and `observability/` cross-cutting.
`backend/fraud/` imports no generated protobuf and no transport — that is what lets
the model be unit-tested with no socket.

---

## Known gaps

Recorded rather than hidden, each with a test that fails when it is fixed:

1. **`POST /orders` does not bind `customer_id` to the token.** Any authenticated
   caller can create an order in another customer's name. The read path *is*
   checked, so the order is unreadable by its creator — but it still reserves
   stock and ships. Fix: compare `request.customer_id` to `principal.subject` with
   a supervisor exemption. See
   `test_an_order_can_be_placed_in_another_customers_name`.
2. **The rolling fraud window is per-process.** With N scoring replicas each sees
   1/N of a customer's velocity. Fix: a shared window store (Redis, or Kafka
   Streams state). Stated in `backend/fraud/profile_window.py`.
3. **`make reconcile` cannot read a live Kafka broker.** It reconciles an exported
   topic (`--log-file`, accepting `kcat -J` output) or a log it generates from its
   own in-process run — so the committed report *is* a real reconciliation by event
   identity rather than "the comparison did not run". A broker client here would
   replace one line of `kcat`.
4. **No refresh tokens, and the user store is a seeded fixture.** Both named in
   `backend/auth/jwt.py`.
5. **The consumer sustains ~35 orders/sec on one process against SQLite.** ~17 SQL
   statements per order, each a cross-thread round trip; not fsync, which was
   measured and ruled out. 500/sec end to end needs the distributed deployment.

---

## Testing

```bash
make test               # everything; integration tests skip themselves if services are down
make test-integration   # needs `make up`
```

Four rules, and the reason for each:

1. `pytest` with nothing running must pass. Infrastructure has in-process
   implementations that are *real* — real offsets, groups, replay, acks,
   dead-letters — not mocks. See [ADR-005](docs/adr/005-in-memory-log-not-a-mock.md).
2. If a test overrides a dependency, another test runs the real one. This is why
   `authenticated_client` exists: the suite default disables auth, so an
   authorisation test would otherwise pass vacuously.
3. No test module basename is reused across directories.
4. No metric label is derived from user input — a label per order id is an
   unbounded time-series store and an attack on the monitoring system.

`tests/conftest.py::concurrent_factory` exists because `sqlite:///:memory:` with
`StaticPool` hands every session the *same* connection, so it cannot express
concurrency: with it, the oversell test grants 182 of 500 and the row ends at
`available = -172`. With a real pool, exactly 10.