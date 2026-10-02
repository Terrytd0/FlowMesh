"""Reconcile the event log against the database.

`scripts/chaos_test.py` proves nothing is lost when a consumer dies mid-stream.
This module answers the same question about a *running* system, and it is the
difference between "the test was green" and "the system agrees with itself".

The question is one sentence: **for every event the log holds, is there a record
that it was applied?** The comparison is by *identity* -- `event_id` against
`processed_events` -- not by count, because a count of 400 against 400 is also
what a pipeline that applied the same event twice and lost two others reports.

**A log that cannot be read is a failure, not a pass.** This is the trap, and it
is the reason this module takes the log as an argument instead of building one:
a broker is not something a script reaches into, so a naive implementation
compares against an empty set, finds no missing events, and reports success --
having verified nothing at all. Here, `log_available=False` makes
`verify()` fail with a message that says so. The first version of this module
returned an empty list for every transport and would have passed on a database
with total data loss.

Three outcomes, all worth knowing:

- **applied**: the normal case
- **missing**: on the log, never applied. Data loss.
- **orphaned**: applied, but not on the log. Should be impossible; non-zero means
  the log was truncated or a handler wrote an effect it did not earn.

`EXPECTED_UNLEDGERED` names the event types that are *correctly* absent from the
ledger, each with its reason. A bare set of suppressions with no explanation is
how a reconciliation report starts reporting success while a bug is live, so a
new entry has to justify itself in the report it appears in.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.clock import isoformat_utc
from backend.core.logging import get_logger
from backend.database.models import Inventory, Order, ProcessedEvent
from backend.events.schema import EventEnvelope, EventType, Topic

logger = get_logger(__name__)

#: What `reconcile` needs from a session factory: a zero-argument callable yielding an
#: async context manager over a session.
#:
#: Spelled as `Callable[[], AsyncContextManager[AsyncSession]]` rather than
#: `async_sessionmaker[AsyncSession]` because that is what it actually uses -- it
#: calls the factory and enters the result -- and naming the narrow type made a
#: deliberate test double a type error, which is a good way to discourage testing the
#: "the database is unreachable" path at all.
SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]

#: Event types that are expected NOT to be in the ledger, with the reason.
#:
#: Keyed by event type, valued by an explanation that appears in the report. A bare
#: set of "expected missing" types with no reason is a set of suppressions, and a
#: suppression is how a reconciliation report starts reporting success while a bug
#: is live.
EXPECTED_UNLEDGERED: dict[str, str] = {
    str(EventType.ORDER_SCORED): (
        "published by the order processor onto the order topic; it is this stage's "
        "output, not its input, and the order-processor group ignores it"
    ),
    str(EventType.ORDER_APPROVED): (
        "terminal decision; consumed by fulfilment, which is not a group in this project"
    ),
    str(EventType.ORDER_REJECTED): (
        "terminal decision; consumed by fulfilment, which is not a group in this project"
    ),
}


@runtime_checkable
class ReadableLog(Protocol):
    """The minimum a log must offer to be reconciled against.

    Two methods, deliberately. `replay` is the whole requirement; a real broker
    offers it too (`kcat -C -o beginning`), and so does the in-process log. What it
    does *not* offer cheaply is the "did this run have a backlog" question, which
    is why that lives in the report rather than in here.
    """

    async def replay(self, topic: Topic, from_offset: int = 0) -> list[EventEnvelope]: ...


class FileLog:
    """A `ReadableLog` over a JSON export of a real broker's records.

    This is what makes the tool able to do its one job against Kafka. The in-process
    log lives in the memory of whatever process published to it, so a *separate*
    reconciliation process cannot read it -- which is why `main()` used to pass no
    log at all and could only ever report "the comparison did not run". That is
    honest but useless: the run still checked the database-side invariants and
    nothing else.

    So the export path is supported rather than merely documented. The report has
    always told an operator to run

        kcat -C -b localhost:9092 -t order-events -o beginning -J > events.json

    and then nothing read that file. `kcat -J` writes newline-delimited JSON whose
    `payload` is a base64-encoded envelope, so both that shape and a plain array of
    envelopes are accepted here; see `_records_from`.

    The point is that the reconciliation is by `event_id`, and an `event_id` survives
    the trip through a text file. That is the whole requirement, and it is why this
    class is 40 lines rather than a Kafka client.
    """

    def __init__(self, path: Path, *, topic: Topic = Topic.ORDER) -> None:
        self._path = path
        self._topic = topic

    async def replay(self, topic: Topic, from_offset: int = 0) -> list[EventEnvelope]:
        if not self._path.is_file():
            raise FileNotFoundError(
                f"{self._path} does not exist. Export the topic first, e.g. "
                f"`kcat -C -b localhost:9092 -t {topic} -o beginning -J > {self._path}`"
            )
        records = _records_from(self._path.read_text(encoding="utf-8"), topic=self._topic)
        envelopes = [
            envelope
            for envelope in records
            if (envelope.offset or 0) >= from_offset and envelope.event_type is not None
        ]
        logger.info("read %s records from %s (topic=%s)", len(envelopes), self._path, self._topic)
        return envelopes


def _records_from(text: str, *, topic: Topic) -> list[EventEnvelope]:
    """Parse envelopes out of an export, in whichever of the three shapes it arrived.

    Accepted, in order of preference:

    - a JSON array of envelopes (`json.dumps([e.model_dump() ...])`);
    - newline-delimited JSON, one envelope per line;
    - `kcat -J` output, where each line is `{"topic":..,"key":..,"payload":"<base64>"}`.

    The last is what the report has always told operators to produce, and it is the
    reason the third shape is here rather than a note saying to convert it by hand.
    """
    stripped = text.strip()
    if not stripped:
        return []

    candidates: list[Any] = []
    try:
        decoded = json.loads(stripped)
        candidates = decoded if isinstance(decoded, list) else [decoded]
    except json.JSONDecodeError:
        for line in stripped.splitlines():
            line = line.strip()
            if line:
                candidates.append(json.loads(line))

    envelopes: list[EventEnvelope] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        payload = candidate.get("payload", candidate.get("value"))
        if isinstance(payload, str):
            try:
                candidate = json.loads(_maybe_base64(payload))
            except (json.JSONDecodeError, ValueError):
                continue
        if not isinstance(candidate, dict):
            continue
        candidate = dict(candidate)
        candidate.setdefault("topic", str(topic))
        try:
            envelopes.append(EventEnvelope.model_validate(candidate))
        except Exception as exc:  # noqa: BLE001 - one bad line must not lose the rest
            logger.warning("skipping an unparseable exported record: %s", exc)
    return envelopes


def _maybe_base64(value: str) -> str:
    """Decode base64 if it decodes to something that looks like JSON, else pass through.

    `kcat` base64-encodes its payload; a hand-rolled export usually does not. Deciding
    by *content* rather than by a flag means the caller does not have to know which
    tool produced the file.
    """
    import base64
    import binascii

    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return value
    try:
        text = decoded.decode("utf-8")
    except UnicodeDecodeError:
        return value
    return text if text.lstrip().startswith(("{", "[")) else value


@dataclass
class ReconciliationReport:
    """The outcome, in a form a human can act on."""

    checked_at: str = ""
    #: False when no readable log was supplied. Makes `verify()` fail.
    log_available: bool = False
    #: False when the database could not be read. Makes `verify()` fail.
    database_available: bool = True
    #: Why the database or the log was unreadable, when one of them was.
    unavailable_reason: str = ""
    total_on_log: int = 0
    applied: int = 0
    missing: int = 0
    orphaned: int = 0
    expected_unledgered: int = 0
    missing_ids: list[str] = field(default_factory=list)
    orphaned_ids: list[str] = field(default_factory=list)
    orders_on_log: int = 0
    orders_in_database: int = 0
    orders_scored: int = 0
    inventory_rows: int = 0
    inventory_inconsistent: int = 0
    negative_stock_rows: int = 0
    passed: bool = False
    failures: list[str] = field(default_factory=list)

    def to_dict(self, *, with_ids: bool = True) -> dict[str, Any]:
        """The machine-readable result.

        `with_ids=False` omits the id lists, which is what the console summary wants:
        a 50-element `missing_ids` array printed above a one-line summary is noise,
        and the ids are in the JSON file and the markdown either way.
        """
        summary: dict[str, Any] = {
            "checked_at": self.checked_at,
            "log_available": self.log_available,
            "database_available": self.database_available,
            "unavailable_reason": self.unavailable_reason,
            "total_on_log": self.total_on_log,
            "applied": self.applied,
            "missing": self.missing,
            "orphaned": self.orphaned,
            "expected_unledgered": self.expected_unledgered,
            "orders_on_log": self.orders_on_log,
            "orders_in_database": self.orders_in_database,
            "orders_scored": self.orders_scored,
            "inventory_rows": self.inventory_rows,
            "inventory_inconsistent": self.inventory_inconsistent,
            "negative_stock_rows": self.negative_stock_rows,
            "passed": self.passed,
            "failures": self.failures,
        }
        if with_ids:
            summary["missing_ids"] = self.missing_ids
            summary["orphaned_ids"] = self.orphaned_ids
        return summary


async def read_log(log: ReadableLog) -> list[EventEnvelope]:
    """Every `order-events` record currently on the log.

    A failure to read is a failure, not an empty list. `replay` raising means the
    log is unreachable, and treating that as "zero events" would turn an outage
    into a clean reconciliation.
    """
    try:
        return await log.replay(Topic.ORDER, from_offset=0)
    except Exception as exc:  # noqa: BLE001 - the failure is the point
        logger.error("could not read the event log: %s", exc)
        raise


async def reconcile(
    *,
    log: ReadableLog | None = None,
    session_factory: SessionFactory | None = None,
) -> ReconciliationReport:
    """Compare the log against the database.

    `log` and `session_factory` are both optional only so the CLI can be run
    against a database with no log to compare -- and `verify()` then *fails*,
    which is the correct outcome for "I could not check".
    """
    from backend.config.settings import get_settings
    from backend.database.session import get_session_factory

    settings = get_settings()
    factory = session_factory or get_session_factory(settings)
    report = ReconciliationReport(checked_at=isoformat_utc(datetime.now(UTC)))

    on_log: list[EventEnvelope] = []
    if log is not None:
        try:
            on_log = await read_log(log)
            report.log_available = True
        except Exception as exc:  # noqa: BLE001 - the failure is the finding
            # An unreadable log is a reconciliation failure, not an empty one --
            # see the module docstring. Recording it as a reason rather than letting
            # it escape is what lets the report still be written, which is the only
            # thing an operator has to go on.
            report.log_available = False
            report.unavailable_reason = f"the event log could not be read: {exc}"
            logger.error("%s", report.unavailable_reason)

    applicable = [
        envelope for envelope in on_log if str(envelope.event_type) not in EXPECTED_UNLEDGERED
    ]
    report.total_on_log = len(on_log)
    report.expected_unledgered = len(on_log) - len(applicable)
    report.orders_on_log = sum(
        1 for envelope in applicable if envelope.event_type == EventType.ORDER_ACCEPTED
    )

    try:
        await _read_database(report, factory, applicable)
    except Exception as exc:  # noqa: BLE001 - same reasoning as the log above
        report.database_available = False
        report.unavailable_reason = (
            f"the database at {settings.database_url.split('://')[0]}://... could not be "
            f"read: {type(exc).__name__}: {exc}"
        )
        logger.error("%s", report.unavailable_reason)

    verify(report)
    return report


async def _read_database(
    report: ReconciliationReport,
    factory: SessionFactory,
    applicable: list[EventEnvelope],
) -> None:
    """Every database-side measure, in one session.

    Split out so a connection failure is caught once, at the call site, instead of
    wrapping each query. The first version of `main()` had no such guard at all: with
    Postgres down, `make reconcile` died with a `socket.gaierror` traceback, wrote no
    report, and exited non-zero from the exception rather than from a finding -- so
    the one situation where an operator most needs the document was the one situation
    that produced none.
    """
    async with factory() as session:
        ledger = await session.execute(select(ProcessedEvent.event_id))
        applied = {row for row in ledger.scalars()}
        report.applied = len(applied)

        # An effect the ledger claims but the log does not contain. A ledger row
        # with a null event id is counted here rather than skipped, because a claim
        # with no evidence behind it is the most serious version of this failure.
        #
        # Only meaningful when a log was actually read: comparing against an empty
        # set would report every ledger row as orphaned and every event as missing,
        # which is the "compared against nothing and reported a count" trap.
        if report.log_available:
            applicable_ids = {envelope.event_id for envelope in applicable}
            missing = sorted(applicable_ids - applied)
            orphaned = sorted(applied - applicable_ids)
            report.missing = len(missing)
            report.orphaned = len(orphaned)
            report.missing_ids = missing[:50]
            report.orphaned_ids = orphaned[:50]

        report.orders_in_database = await _count(session, Order)
        report.orders_scored = await _count(session, Order, extra=Order.fraud_score.is_not(None))
        report.inventory_rows = await _count(session, Inventory)
        report.inventory_inconsistent = await _count(
            session,
            Inventory,
            extra=Inventory.available != (Inventory.on_hand - Inventory.reserved),
        )
        report.negative_stock_rows = await _count(session, Inventory, extra=Inventory.available < 0)


async def _count(session: AsyncSession, model: Any, *, extra: Any = None) -> int:
    statement = select(func.count()).select_from(model)
    if extra is not None:
        statement = statement.where(extra)
    result = await session.execute(statement)
    return int(result.scalar_one())


def verify(report: ReconciliationReport) -> None:
    """Every check, named. A verifier that reports one failure hides the rest."""
    failures = report.failures

    if not report.database_available:
        failures.append(
            report.unavailable_reason
            + ". Nothing below was measured, so this run proves nothing -- check that "
            "the database is running and FLOWMESH_DATABASE_URL points at it."
        )

    if not report.log_available:
        failures.append(
            "no readable event log was supplied, so the log-to-database comparison "
            "did not run; this report proves only the database-side invariants. "
            "Export the topic and pass it with --log-file: "
            "`kcat -C -b localhost:9092 -t order-events -o beginning -J > events.json` "
            "then `make reconcile LOG_FILE=events.json`."
        )

    if report.missing:
        failures.append(
            f"{report.missing} events on the log were never applied to the database "
            f"(first few: {report.missing_ids[:5]})"
        )
    if report.orphaned:
        failures.append(
            f"{report.orphaned} database effects reference events that are not on "
            f"the log (first few: {report.orphaned_ids[:5]})"
        )
    if report.negative_stock_rows:
        failures.append(
            f"{report.negative_stock_rows} inventory rows have negative available stock"
        )
    if report.inventory_inconsistent:
        failures.append(
            f"{report.inventory_inconsistent} inventory rows do not satisfy "
            "available = on_hand - reserved"
        )
    if report.log_available and report.orders_on_log and not report.orders_in_database:
        failures.append("the log has order events but the database has no orders at all")

    report.passed = not failures


def render_markdown(report: ReconciliationReport) -> str:
    verdict = "PASS" if report.passed else "FAIL"
    lines = [
        "# FlowMesh reconciliation",
        "",
        f"**Result: {verdict}**",
        "",
        f"Checked {report.checked_at} by `make reconcile`.",
        "",
        "## What this compares",
        "",
        "Every event on the event log against every row in the `processed_events`",
        "ledger, **by identity**. A count would not do: 400 applied and 400 published",
        "is also what a pipeline that applied two events twice and lost two others",
        "reports.",
        "",
        "| measure | value |",
        "| --- | --- |",
        f"| event log was readable | "
        f"{'yes' if report.log_available else 'NO -- comparison skipped'} |",
        f"| database was readable | "
        f"{'yes' if report.database_available else 'NO -- nothing measured'} |",
        f"| events on the log | {report.total_on_log} |",
        f"| applied to the database | {report.applied} |",
        f"| **on the log but never applied** | **{report.missing}** |",
        f"| **applied but not on the log** | **{report.orphaned}** |",
        f"| expected to be unledgered | {report.expected_unledgered} |",
        f"| orders on the log | {report.orders_on_log} |",
        f"| orders in the database | {report.orders_in_database} |",
        f"| orders scored | {report.orders_scored} |",
        f"| inventory rows | {report.inventory_rows} |",
        f"| **rows with negative stock** | **{report.negative_stock_rows}** |",
        f"| **rows where available != on_hand - reserved** | **{report.inventory_inconsistent}** |",
        "",
        "## Event types expected not to be in the ledger",
        "",
        "Named explicitly, with the reason, so a new one cannot be added quietly and",
        "start suppressing real losses:",
        "",
    ]
    for event_type, reason in EXPECTED_UNLEDGERED.items():
        lines.append(f"- **`{event_type}`** -- {reason}")
    lines += [
        "",
        "## Scope, and the trap",
        "",
        "The log-to-database comparison needs a log this process can read. The in-process",
        "log lives in the memory of the process that published to it, so a separate",
        "reconciliation process cannot see it. Against a real Kafka broker, export the",
        "topic and pass the file in:",
        "",
        "```bash",
        "kcat -C -b localhost:9092 -t order-events -o beginning -J > events.json",
        "python -m backend.scripts.reconcile --log-file events.json",
        "```",
        "",
        "A script that cannot read the log and then reports `missing: 0` has proved",
        "nothing at all -- and that is the failure mode of every reconciliation tool",
        "that reports a count. So this one treats an unreadable log as a **failure**,",
        "and says which checks actually ran. The database-side invariants (negative",
        "stock, the `available = on_hand - reserved` identity) need no log and are",
        "always checked.",
        "",
        "A database that cannot be reached is reported the same way, rather than as a",
        "traceback: a crash writes no report, and the run that most needs the document",
        "is the run where nothing is listening.",
        "",
    ]
    if report.missing_ids:
        lines += [
            "### Unapplied events (sample)",
            "",
            "```json",
            json.dumps(report.missing_ids[:20], indent=2),
            "```",
            "",
        ]
    if report.failures:
        lines += ["## Failures", "", *[f"- {failure}" for failure in report.failures], ""]
    lines += [
        "## Reproducing",
        "",
        "```bash",
        "make reconcile",
        "```",
        "",
        "Exits non-zero on any failure.",
        "",
    ]
    return "\n".join(lines)


async def _generate_export(path: Path, *, orders: int) -> tuple[list[Any], dict[str, Any]]:
    """Run a small stream through the real pipeline and write its log to `path`.

    This is what makes `make reconcile` able to do its one job on the topology it can
    actually reach. The in-process event log lives in the memory of the process that
    published to it, so a *separate* reconciliation process cannot read it -- which
    meant `make reconcile` could only ever report "the comparison did not run", and
    the third evidence script produced a document that proved nothing about the log.
    That is an honest limitation, but as the *only* outcome it is not a finished
    tool.

    So the default single-process mode generates the export itself: the same
    processors, the same SQL and the same idempotency ledger as `make loadtest`, run
    over `orders` synthetic orders, drained to completion, and then read back out of
    `InMemoryEventLog.published_envelopes()`. What is reconciled is therefore the real
    log of a real run, not a fixture.

    Against a real Kafka cluster and a real Postgres, skip all of this and use
    `kcat` plus `--from-stream 0`; `--log-file` accepts that output in the same `-J`
    shape written here.

    Returns `(envelopes, pipeline)` so the reconciliation below reads **the same
    database the log came from**. Passing the export while reconciling some other
    database -- the one in `FLOWMESH_DATABASE_URL` -- would compare a log against an
    unrelated store and report every event as missing, which is worse than not
    reconciling at all.
    """
    from backend.loadtest.harness import build_pipeline, drain, drive

    directory = path.parent / "reconcile-run"
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / "reconcile.sqlite3"
    # Removed first, because `build_pipeline` seeds the catalogue and a previous run's
    # rows would fail the `UNIQUE (warehouse_id, sku)` constraint. The alternative --
    # keeping them and reconciling the accumulation of every run -- is a different
    # measurement, and a confusing one: the report would describe a database no run
    # produced. A reconciliation has to describe *this* run.
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{database}{suffix}")
        if candidate.exists():
            candidate.unlink()

    pipeline = await build_pipeline(database_path=database, partitions=4)
    try:
        # `target_rate == orders` over one second publishes exactly `orders` of them,
        # so the caller asks for a number of orders and gets that number rather than
        # one that depends on the clock. The consumer then has to catch up, which is
        # the point: reconciliation only means anything once the backlog is empty.
        await drive(pipeline=pipeline, target_rate=max(orders, 1), duration_seconds=1.0)
        await drain(pipeline=pipeline, timeout=_drain_deadline_seconds())
        envelopes = pipeline["bus"].published_envelopes()
    except BaseException:
        await _teardown(pipeline)
        raise

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(_as_kcat_record(envelope)) for envelope in envelopes),
        encoding="utf-8",
    )
    logger.info("wrote %s records to %s", len(envelopes), path)
    # The engine is deliberately not disposed here: the reconciliation reads this
    # database. `_teardown` runs once the report is written.
    return envelopes, pipeline


async def _teardown(pipeline: dict[str, Any]) -> None:
    """Release the generated pipeline. Best-effort, so teardown cannot mask a finding."""
    from contextlib import suppress

    with suppress(Exception):
        await pipeline["bus"].cancel(pipeline["handle"])
    with suppress(Exception):
        await pipeline["queue"].stop()
    with suppress(Exception):
        await pipeline["bus"].stop()
    with suppress(Exception):
        await pipeline["engine"].dispose()


def _as_kcat_record(envelope: EventEnvelope) -> dict[str, Any]:
    """One envelope in `kcat -J`'s shape: a base64 `payload` beside the key.

    Written in the broker's format rather than the model's own, so that the file this
    produces and the file `kcat` produces are the same artefact -- which is what makes
    `--log-file` a real interface rather than two formats with a shared name.
    """
    import base64

    return {
        "topic": str(Topic.ORDER),
        "key": envelope.order_id,
        "payload": base64.b64encode(envelope.model_dump_json().encode("utf-8")).decode("ascii"),
    }


def _drain_deadline_seconds() -> float:
    """The load test's drain deadline, so the two tools cannot drift apart."""
    from backend.scripts.loadtest import Budget

    return Budget().drain_deadline_seconds


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile the event log against the database.")
    parser.add_argument("--report", type=Path, default=Path("docs/reconciliation.md"))
    parser.add_argument("--json", type=Path, default=Path("data/runtime/reconciliation.json"))
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help=(
            "a JSON export of the order topic to reconcile against. Accepts a kcat -J "
            "export, newline-delimited envelopes, or a JSON array of them. Defaults "
            "to a file this script generates from its own pipeline run."
        ),
    )
    parser.add_argument(
        "--from-stream",
        type=int,
        default=200,
        help=(
            "orders to push through the in-process pipeline before reconciling, so "
            "the export is the log of a real run rather than a fixture. Use 0 with "
            "--log-file to reconcile an external export without generating one."
        ),
    )
    args = parser.parse_args()

    from backend.core.logging import configure_logging

    configure_logging("WARNING")

    async def run() -> ReconciliationReport:
        log_file = args.log_file or Path("data/runtime/order-events.json")
        generated: dict[str, Any] | None = None
        session_factory = None
        if args.from_stream > 0 and args.log_file is None:
            _envelopes, generated = await _generate_export(log_file, orders=args.from_stream)
            session_factory = generated["session_factory"]
        try:
            # An unreadable log is reported, not raised: see `reconcile()`.
            return await reconcile(log=FileLog(log_file), session_factory=session_factory)
        finally:
            if generated is not None:
                await _teardown(generated)

    report = asyncio.run(run())

    print(json.dumps(report.to_dict(with_ids=False), indent=2))  # noqa: T201
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(render_markdown(report), encoding="utf-8")
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    raise SystemExit(0 if report.passed else 1)


# See the note on the same guard in `backend/scripts/loadtest.py`. Worst case of the
# three: `make reconcile` exiting 0 with no report written reads as "reconciliation
# passed", and reconciliation is the check that proves nothing was lost.
if __name__ == "__main__":
    main()
