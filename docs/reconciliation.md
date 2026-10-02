# FlowMesh reconciliation

**Result: PASS**

Checked 2026-10-02T06:04:46.321410Z by `make reconcile`.

## What this compares

Every event on the event log against every row in the `processed_events`
ledger, **by identity**. A count would not do: 400 applied and 400 published
is also what a pipeline that applied two events twice and lost two others
reports.

| measure | value |
| --- | --- |
| event log was readable | yes |
| database was readable | yes |
| events on the log | 302 |
| applied to the database | 151 |
| **on the log but never applied** | **0** |
| **applied but not on the log** | **0** |
| expected to be unledgered | 151 |
| orders on the log | 151 |
| orders in the database | 151 |
| orders scored | 151 |
| inventory rows | 72 |
| **rows with negative stock** | **0** |
| **rows where available != on_hand - reserved** | **0** |

## Event types expected not to be in the ledger

Named explicitly, with the reason, so a new one cannot be added quietly and
start suppressing real losses:

- **`order.scored`** -- published by the order processor onto the order topic; it is this stage's output, not its input, and the order-processor group ignores it
- **`order.approved`** -- terminal decision; consumed by fulfilment, which is not a group in this project
- **`order.rejected`** -- terminal decision; consumed by fulfilment, which is not a group in this project

## Scope, and the trap

The log-to-database comparison needs a log this process can read. The in-process
log lives in the memory of the process that published to it, so a separate
reconciliation process cannot see it. Against a real Kafka broker, export the
topic and pass the file in:

```bash
kcat -C -b localhost:9092 -t order-events -o beginning -J > events.json
python -m backend.scripts.reconcile --log-file events.json
```

A script that cannot read the log and then reports `missing: 0` has proved
nothing at all -- and that is the failure mode of every reconciliation tool
that reports a count. So this one treats an unreadable log as a **failure**,
and says which checks actually ran. The database-side invariants (negative
stock, the `available = on_hand - reserved` identity) need no log and are
always checked.

A database that cannot be reached is reported the same way, rather than as a
traceback: a crash writes no report, and the run that most needs the document
is the run where nothing is listening.

## Reproducing

```bash
make reconcile
```

Exits non-zero on any failure.
