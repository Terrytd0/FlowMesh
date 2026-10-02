"""Time sources.

Every `datetime.now()` in this codebase is a bug waiting to happen, because a
pipeline is a distributed system and its events cross process boundaries. UTC
only, timezone-aware only, monotonic for durations.

`utcnow()` is a function rather than a constant so tests can pin time with
`freeze_time` (or by calling `set_clock_offset`) instead of sleeping.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

_clock_offset: timedelta = timedelta(0)


def utcnow() -> datetime:
    """Timezone-aware current UTC time.

    Aware by construction. Naive datetimes are the single most common source of
    off-by-hours bugs in a pipeline whose events are produced in one container
    and consumed in another, because a naive value silently adopts whatever
    local time the consuming process happens to have.
    """
    return datetime.now(UTC) + _clock_offset


def monotonic() -> float:
    """Monotonic seconds, for measuring durations only.

    Never for a timestamp. `time.monotonic()` has no relationship to wall-clock
    time and must not be written into a record that a human will read.
    """
    return time.monotonic()


def isoformat_utc(value: datetime) -> str:
    """RFC 3339 / ISO 8601 in UTC, with a `Z` suffix.

    Hand-rolled because `datetime.isoformat()` renders UTC as `+00:00` and
    `Kafka` consumers, `date` parsers and most log aggregators prefer `Z`. The
    event payloads on the wire all go through here so there is one rendering.
    """
    aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return aware.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_isoformat_utc(value: str) -> datetime:
    """Inverse of `isoformat_utc`, tolerant of a `Z` suffix and of `+00:00`.

    Tolerating both is deliberate: the events this project writes use `Z`,
    Kafka's own console tooling writes `+00:00`, and an ingestion pipeline that
    crashes on the second one is not ingestible by a human with `kcat`.
    """
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def as_utc(value: datetime | None) -> datetime | None:
    """Coerce a datetime from the database to aware UTC.

    **Load-bearing, and it exists because SQLite does not store timezones.**

    `DateTime(timezone=True)` is a promise to the ORM, not to the storage engine.
    PostgreSQL honours it and hands back aware datetimes; SQLite has no timezone
    concept at all and hands back *naive* ones. So the same query returns two
    different types depending on which backend answered it, and the first thing
    that breaks is a comparison:

        `expires_at < utcnow()`   ->  TypeError: can't compare offset-naive and
                                     offset-aware datetimes

    which is exactly what the expiry sweeper does. Rather than sprinkling
    `or replace(tzinfo=UTC)` across every comparison -- and missing one the first
    time a new query is written -- every value read from the database goes through
    here. Naive values are *assumed* UTC, which is correct for this project
    because `utcnow()` is the only clock that writes them.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def age_seconds(value: datetime | None, *, now: datetime | None = None) -> float:
    """Seconds between `value` and now, tolerant of either being naive.

    Used for every "how old is this" calculation on the review dashboard. Same
    reason as `as_utc`: a dashboard that raises on one backend and works on
    another is a dashboard nobody trusts.
    """
    if value is None:
        return 0.0
    reference = as_utc(now or utcnow())
    assert reference is not None
    return (reference - as_utc(value)).total_seconds()  # type: ignore[operator]


def set_clock_offset(offset: timedelta) -> None:
    """Shift `utcnow()` by `offset`. Tests only.

    Exists for SLA and reservation-expiry paths, which are otherwise only
    testable by sleeping -- and a test suite that sleeps is a test suite that
    is skipped in CI.
    """
    global _clock_offset
    _clock_offset = offset


def reset_clock_offset() -> None:
    """Undo `set_clock_offset`."""
    set_clock_offset(timedelta(0))
