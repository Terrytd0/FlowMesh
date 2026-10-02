# ADR-004: Kafka and RabbitMQ, or one of them twice

**Status:** accepted
**Date:** 2026-09-29
**Context:** Sprint 7 — FlowMesh, real-time order and fraud pipeline

## The question

The roadmap says Kafka *and* RabbitMQ. The obvious reaction is that this is
redundant: two brokers, two failure modes, two sets of operational knowledge, for
what looks like one job. This ADR records why running both is correct here, and
what would have gone wrong with either one used for both jobs.

The short answer: they are not two implementations of the same abstraction. One is
a **log of what happened**. The other is a **queue of what still needs doing**.
This pipeline needs both, and the requirements that force the second broker are
sharp enough to name.

## The two failure modes

### Kafka: a durable, ordered, replayable log

Kafka's unit is a record at an offset in a partition. Its guarantees:

- **Durable** — replicated, retained for a configured window
- **Ordered per partition** — all records with the same key land on one partition
  and are delivered in order
- **Replayable** — a consumer's position is an offset, and a new consumer group can
  read from any offset
- **High throughput** — the design target is millions of records/sec

What it does **not** give you:

- **Per-message acknowledgement.** Kafka's consumer offset *is* the ack, and it
  moves at the granularity of a partition, not a message. A handler that processes
  three messages and fails on the fourth has no way to say "two of those are done".
- **Dead-lettering.** There is no built-in concept of "this message cannot be
  processed, put it somewhere a human will look". You build it, and the usual
  implementation is a `try/except` that publishes to another topic — which is a
  queue you just implemented yourself, badly.
- **Routing.** Records go to a topic. There are no headers, no priority, no
  per-message TTL.

### RabbitMQ: an acknowledged, routable, dead-lettered queue

RabbitMQ's unit is an unacknowledged message in a queue. Its guarantees:

- **Per-message ack/nack** — the consumer decides, per message
- **Redelivery on nack** — with a retry count, bounded by policy
- **Dead-letter exchange** — a nacked-past-the-limit message goes *somewhere
  specific*, by configuration
- **Routing** — exchanges, bindings, routing keys, per-message TTL, priorities
- **Push delivery with backpressure** — prefetch tells the broker how much
  unacknowledged work a consumer may hold

What it does **not** give you:

- **Replay.** A message that is acknowledged is gone. There is no "read this queue
  from the beginning again".
- **Ordering across consumers.** Two consumers on one queue get an arbitrary split,
  not a total order.

## Why this pipeline needs both

### Kafka owns the event log

`order-events` and `inventory-events` are facts: `order.accepted`,
`order.scored`, `inventory.reserved`. Each is an immutable statement about
something that happened.

Three requirements, all of them Kafka:

1. **Replay.** The chaos test's central claim is that a killed consumer resumes
   without loss. That requires the log to still hold what the consumer had not yet
   processed. A queue cannot do this — an acked message is gone, and an *unacked*
   one is only delivered to the consumer that holds it.

2. **New consumers from history.** Sprint 11's Aegis governance layer is specified
   to ingest events from previous systems. Reading six weeks of `order-events` from
   a retained log is one consumer group starting at offset 0. On a queue it is
   impossible by construction.

3. **Ordering per key.** All of one order's events must land on one consumer, in
   order, or `order.scored` can arrive before `order.accepted`. That is partition
   keying, which is Kafka's model. RabbitMQ preserves FIFO per queue, which would
   serialise the entire pipeline behind one order.

### RabbitMQ owns the review queue

`review.requested` is a **task**: a human (or the LLM reasoner) needs to look at
this order. This is the requirement that decides the question, and it is worth
stating precisely, because a Kafka-only design can be built that appears to work.

**If `review.requested` is lost, a fraudulent order sits unscored and the customer
is charged.** A risk-scored order whose score never arrived is an unscored order.
There is no compensating mechanism — nobody notices a review that never happened.

**If it is replayed three hours later, it is worse than useless.** A reviewer sees
an order that has already been decided, and either wastes their time or (worse)
acts on stale context.

So the requirement is: *exactly-once delivery to a consumer, with a bounded number of
attempts, and a definite resting place for the failures.* That is precisely
RabbitMQ's vocabulary and not Kafka's:

| requirement | Kafka | RabbitMQ |
| --- | --- | --- |
| acknowledged when handled | at partition granularity | per message |
| retried on failure | consumer seeks back | broker redelivers on nack |
| bounded retries | consumer-side loop | `x-death` count, or the delivery limit |
| final resting place | you build a DLQ topic | `x-dead-letter-exchange`, configured |
| retry limit visible to ops | not built in | queue depth by DLQ |
| operator can see poison messages | consumer's own logs | a queue you can open |

The dead-letter exchange is the part that is genuinely hard to build on Kafka. The
mechanism is a `try/except` that publishes to a `review.dlq` topic — and now you
have two things to get right that RabbitMQ gives you as configuration: the retry
count, and the fact that a message which cannot be processed ends up somewhere
durable and inspectable rather than being retried forever by a consumer that is
wedged.

## What we rejected, and why

### One broker: Kafka for everything

Tempting, and it halves the operational surface. Rejected because the review path
needs per-message acknowledgement and a dead-letter resting place, and on Kafka
those are two things you build yourself, in the place where a mistake is least
likely to be noticed — inside an exception handler in a consumer.

Concretely, the failure we would have had to design around: a `review.requested`
message whose handler throws because the review row write lost a race. The
consumer logs and seeks back. Another message behind it in the same partition is now
blocked behind a poison record, because ordering means it cannot be delivered until
the partition head clears. One bad record stalls every subsequent order in that
partition. On RabbitMQ the same message is nacked, retried twice, and dead-lettered,
and the queue keeps moving.

### One broker: RabbitMQ for everything

Also rejected, and this is the more tempting of the two because a queue is the
easier abstraction to hold in your head.

- **No replay**, so the chaos test's claim is untestable and the reconciliation
  report has nothing to reconcile against.
- **No history**, so a new consumer cannot be introduced without re-publishing.
- **Ordering is per queue, not per key**, so "all of one order's events in order"
  becomes "all orders in order", which caps the pipeline at one consumer.

### gRPC for the review queue

Rejected on a different axis. gRPC is a *request/response* protocol; using it for
work that must survive a consumer restart means reimplementing a queue with
retries, and getting the retry semantics wrong in the process. A held message is not
a request that timed out — it is work that nobody has done yet.

### Redis Streams for the review queue

A serious contender, and the closest call here. Redis Streams is a log with
consumer groups and acknowledgement, which covers replay and durability in one
dependency, and it is one fewer broker to run. Rejected because:

- **Group state lives in Redis**, so consumer-group bookkeeping shares fate with
  the data. A Redis failover takes the review queue's *bookkeeping* down, not just
  its messages, and the recovery path is less well-trodden than RabbitMQ's.
- **Dead-lettering is still DIY.** Same objection as Kafka.
- **We already run Redis** for rate limiting, so the marginal saving is one broker
  rather than two — but the rate limiter degrades to *allow* when Redis is down,
  whereas a review queue must not lose messages.

## What this costs, honestly

Two brokers is two more things to run, monitor, upgrade and be on call for. That is
a real cost and this ADR does not argue otherwise. The justification is that the
alternative is not "one broker" — it is one broker *plus* a hand-rolled queue inside
the consumer, with the retry count and the dead-letter queue as code nobody reviews
as carefully as the broker config they replace.

And the honest counter-argument, which we accepted: **if this project had only
`order-events` and no human review step, Kafka alone would be the right answer.**
The second broker is not a general best practice. It is the specific answer to "a
fraudulent order must never sit unscored, and a message that cannot be processed
must end up somewhere a person will find it."

## Consequences

- `backend/events/bus.py` and `backend/queues/protocol.py` are two separate ports,
  not one abstraction with two backends. Collapsing them would be the first bad
  refactor to make.
- `ensure_topics` and `declare` are separate calls. Starting the system creates
  topics in one and queues in the other.
- The in-process implementations exist for both, and both are *real*
  (see [ADR-005](005-in-memory-log-not-a-mock.md)) — the in-process queue has acks,
  redelivery and a dead-letter queue precisely so the consumer code can be tested
  without a broker.
- Two places to look when asking "did my event get processed?" — the ledger
  (`processed_events`) for the log, and the review queue's depth for the tasks. The
  reconciliation script's `EXPECTED_UNLEDGERED` map is where the difference is
  recorded, with a reason per event type.