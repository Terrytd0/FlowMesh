# Integration tests

These are the tests that need something outside the process. Everything else in the
suite runs against the in-process transports and needs nothing started.

```bash
make test               # the whole suite; these skip themselves if nothing is running
make test-integration   # only these, against a live stack
```

## The rule that matters here

**A bare `pytest` with no flags and no services running must pass.** A test that
fails because Postgres is down trains people to ignore red, and then the red that
matters gets ignored too. So a test in this directory checks whether its dependency
is reachable and *skips* if it is not — it never fails for someone else's absence.

`pytest -m integration` inverts the intent on purpose: run against a stack, an
unreachable dependency **is** the failure, because you asked for these tests
specifically.

## What is actually covered

| dependency | what it proves that the unit suite cannot |
| --- | --- |
| PostgreSQL | the conditional `UPDATE ... WHERE available >= :qty` under real row locks, not SQLite's single-writer emulation. The oversell invariant is a *database* claim and only a real database can be asked. |
| Kafka | partition assignment by `crc32(key)`, committed offsets surviving a consumer restart, and real rebalance timing when a member dies. The in-process log has positions, not rebalances — see [ADR-005](../adr/005-in-memory-log-not-a-mock.md). |
| RabbitMQ | publisher confirms, redelivery on `nack`, and the dead-letter exchange actually dead-lettering. The in-process queue models the routing but not the broker's own recovery. |
| gRPC | the protobuf round trip and the status-code mapping in [ADR-007](../adr/007-grpc-status-codes.md), including the deadline path the chaos test explicitly does not cover. |

Note what is **not** here: throughput. A broker on a laptop measures the laptop.
The latency and throughput numbers in `docs/load-test-report.md` are from the
single-process harness and say so on their face.

## Running against the compose stack

```bash
make up                       # kafka · rabbitmq · postgres · api · fraud · workers
docker compose exec api python -m backend.scripts.seed
make test-integration
```

For the database-backed tests, point the suite at the container's Postgres rather
than SQLite:

```bash
export FLOWMESH_DATABASE_URL="postgresql+asyncpg://flowmesh:flowmesh@localhost:5432/flowmesh"
make test-integration
```

## Adding one

Two obligations, both of which have been got wrong here before:

1. **Skip when the dependency is absent.** Use a reachability probe and
   `pytest.skip`, and say what you skipped for. A test that fails on an absent
   service breaks CI for everyone who did not run `make up`.
2. **Do not reuse a module basename from another directory.** `tests/` has no
   `__init__.py`, so pytest's rootdir-based module naming collides: two
   `test_orders.py` files in different directories is an import error that has
   nothing to do with either test.

Mark it so the default run picks it up correctly:

```python
pytestmark = pytest.mark.integration
```

## What a skip must not hide

A skip is not a pass. If a test here has been skipping for months because a
container name changed, the suite is green and the coverage is gone. When you add a
test, run it once against a real stack and confirm it actually executes — the
failure mode of a test that never runs is that it never fails either.