# ADRs

Architecture decision records for FlowMesh. Each one records a decision, the
alternatives that were rejected, and — where the reasoning is non-obvious — how the
decision was wrong the first time.

Read these before changing anything structural. Re-proposing a rejected alternative
is how these get undone, and each ADR says what the alternative would have cost.

| ADR | decision |
| --- | --- |
| [004 — Kafka and RabbitMQ](004-kafka-vs-rabbitmq.md) | why this pipeline runs two brokers, and what would have broken with either one used for both jobs |
| [005 — the in-process log is not a mock](005-in-memory-log-not-a-mock.md) | why the test doubles have real offsets, and exactly where they differ from the real thing |
| [006 — an LLM cannot meet a 200ms p95](006-llm-on-the-hot-path.md) | why the reasoner is off by default and confined to the ambiguous band |
| [007 — gRPC status codes](007-grpc-status-codes.md) | the failure-to-code mapping, and the two ways the boundary was wrong first |

## Why ADRs start at 004

The sprint numbering follows the roadmap, and the roadmap builds on three existing
projects. ADRs 001–003 are therefore not "lost" — they belong to those repositories
and their reasoning carries over:

- **001–002** (agent frameworks, safety rails) live in
  `supportops-ai/docs/adr/` and `SentinelMCP/docs/adr/`. The load-bearing ideas
  this project reuses: a single stated rule per system, enforced by a test that
  fails in a way that names what was broken.
- **003** (gRPC boundaries) lives in `SentinelMCP/docs/adr/003-grpc-boundary.md`.
  The status-mapping discipline in ADR-007 is the same one, applied to a different
  service.

Numbering them from 004 keeps a single cross-referenced sequence across the
portfolio rather than restarting at 001 in a fifth repository.

## What is deliberately not an ADR

Some things are recorded in `CLAUDE.md` instead, because they are invariants rather
than decisions:

- the conditional `UPDATE` that prevents overselling
- `mark_processed()` as an INSERT in the caller's transaction
- `UtcDateTime` instead of `DateTime(timezone=True)`
- labelled-metric API (`labels(...)`)

Those are consequences of decisions recorded here. Writing an ADR for each would
bury the four decisions that actually had alternatives.