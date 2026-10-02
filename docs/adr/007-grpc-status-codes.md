# ADR-007: the gRPC status codes, and two ways the boundary was wrong first

**Status:** accepted
**Date:** 2026-09-29
**Context:** Sprint 7 — FlowMesh, the fraud-scoring service boundary

## The decision

`backend/grpc_service/` is a typed boundary between the order pipeline and the
service that decides whether an order is worth a human's time. The mapping from
failure to gRPC status code is the part of that boundary worth writing down, because
the code chosen determines what the caller does next, and the wrong choice turns a
recoverable situation into a dropped order.

| situation | code | what the caller should do |
| --- | --- | --- |
| empty `order_id`, empty `customer_id`, negative total, zero quantity, no items | `INVALID_ARGUMENT` | do not retry; the request is malformed |
| deadline elapsed | `DEADLINE_EXCEEDED` | retry elsewhere, not here |
| scorer not ready / model not loaded | `UNAVAILABLE` | try another replica |
| anything else | `INTERNAL` | this is a bug; logged with a stack trace |

## Why each code, and what the wrong one would have done

### `INVALID_ARGUMENT` for malformed requests

The caller sent something it will send again, unchanged. A retry is not merely
useless here, it is harmful: it re-runs the handler, re-publishes nothing, and
consumes a scoring slot. Worse, an `INTERNAL` would tell the caller this is a bug
worth alerting on, and it would alert on every malformed request from a buggy
client.

Validation is in `FraudScoringServicer._validate`, which returns a human-readable
reason. "item 'SKU-A' has quantity 0, must be >= 1" is worth more to whoever fixes
the client than `INVALID_ARGUMENT`.

### `DEADLINE_EXCEEDED` when the deadline elapses

Distinct from `UNAVAILABLE` on purpose. The distinction is about *where* to retry:
`UNAVAILABLE` means "this replica is not ready, try another one", and
`DEADLINE_EXCEEDED` means "this replica was slow". Retrying an `UNAVAILABLE`
immediately against the same replica is pointless but harmless; retrying a timeout
immediately is how a scorer under load turns a slow path into a collapsed one.

The deadline is 2 seconds (`grpc_timeout_seconds`), against a 200ms p95 budget. That
is deliberately generous: the deadline is a backstop for a dead socket, not the
budget. The budget is measured and asserted; the deadline is what stops a request
from waiting forever.

The client's `AutoFraudClient` retries **only** `UNAVAILABLE`, and **only** on the
other transport. Retrying `INVALID_ARGUMENT` would be a poison-message loop;
retrying `DEADLINE_EXCEEDED` would double the load on a scorer that is already too
slow.

### `UNAVAILABLE` for an unloaded model

This is the load-bearing choice.

`FraudScoringEngine` does **not** refuse to start when its weights file is missing.
It loads the `HeuristicFallback`, marks every decision `degraded`, and starts
serving — because a fraud scorer that will not boot stops all order processing, and
a worse outage than a weaker model.

That decision creates a state: a replica that is running, answering `Health`, and
scoring with a model nobody would choose deliberately. `FAILED_PRECONDITION` would
be the intuitive code for it, and it is the wrong one. A gRPC client that sees
`FAILED_PRECONDITION` treats the request as *this replica's* problem and typically
drops it. A dropped order is a lost customer's order. `UNAVAILABLE` says "not ready
here", which is exactly true and is what a failover-aware client acts on.

Two layers keep this safe: the fallback is a real (if weaker) scorer rather than a
zero vector, **and** `route_for()` refuses to auto-approve any `degraded` decision
regardless of its band or score. See invariant 10 in `CLAUDE.md`.

### `INTERNAL` for everything else

Logged with a stack trace, so the failure is diagnosable. The alternative — mapping
unknown failures to `UNAVAILABLE` — would make real bugs look like cold starts, and
a cold start does not page anybody.

## Two ways the boundary was wrong first

Both are recorded because the reasoning that fixed them is the part worth keeping.

### 1. `to_wire()` returned a dict instead of bytes

The first `EventEnvelope.to_wire()` was annotated `-> bytes` and returned
`self.model_dump(mode="json")` — a dict. Pydantic accepted it, mypy did not (the
annotation was wrong in a way that happened to typecheck against `dict[str, Any]`
at the call site), and every test that did not go through a real broker passed.

The failure would have appeared the first time a message was published to Kafka:
either a serialisation error, or — worse — a client that serialised the envelope a
second time with whatever default its JSON encoder happened to use, producing bytes
that no `from_wire()` on another service could parse.

Both forms now exist with distinct names: `to_wire() -> bytes` and
`to_dict()` for the in-process log. The type annotation and the return value
agree, and `tests/unit/events/test_memory_log.py::test_envelope_round_trips_through_json`
round-trips through the byte form.

### 2. The status mapping was not enforced at the client

`AutoFraudClient` originally retried on any `FraudScoringError`. That made a
malformed order retry against the other transport, and then retry again — a
poison-message loop driven by a client bug, in the component whose whole job is to
be careful with orders.

Fixed by branching on `exc.code`: `UNAVAILABLE` (and `None`, for a non-gRPC
failure) retries, everything else propagates. The rule is now stated in the class
docstring as well, because "retry which codes?" is exactly the question a future
edit needs to re-answer.

## What the client adds

`AutoFraudClient` probes `Health` once and then commits to whichever scorer
answered. Per-call probing would double the requests on the hot path and would
still race with the answer it just got.

The cost is that a scorer which dies *after* the probe is not re-discovered by the
probe — which is why `score()` retries the other transport once on `UNAVAILABLE`.
A dead scorer is recovered by the call path, not the health path.

The fallback logs at WARN and every decision it returns carries
`transport="in_process"`, which is a label on the latency histogram and a series in
Grafana. An operator can therefore see the "grpc" latency panel go flat because
the calls stopped being gRPC, rather than inferring it from a suspiciously round
number.

## What this boundary does not cover

- **No mutual TLS or authentication.** Both services are inside the compose
  network. A real deployment would need either mTLS or a service mesh; this is
  named in `docs/architecture.md` rather than left implicit.
- **No load balancing across replicas.** The client takes one target from settings.
  Kubernetes service discovery replaces the DNS name, but the client does no health
  list management.
- **No protobuf versioning.** `proto/flowmesh/v1/` — the `v1` is a convention
  honouring the package layout, not a supported compatibility policy. Adding a
  field to `FraudRequest` is wire-compatible; removing one is not, and nothing
  detects the break. Aegis (Sprint 11) is where a fleet-wide policy belongs.