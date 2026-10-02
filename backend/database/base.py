"""Declarative base and the shared mixins.

`Base` is what every model inherits and what Alembic's `target_metadata` points
at. Two conventions worth stating because they are easy to get wrong when the
schema grows:

- **Money is integer cents, everywhere.** No floats anywhere in this schema. A
  float cent column loses a cent per few thousand rows and the reconciliation
  report -- "does the ledger balance?" -- is precisely the check that would
  notice, which is to say it would fail on data nobody else could explain.
- **`id` is a string everywhere.** Order ids, reservation ids and event ids are
  all externally generated and must be identical in the database, on the event
  log and in a customer's support ticket. A surrogate integer key on top would
  create a second identity for every row and a permanent source of "which id did
  I use in that log line" bugs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import DateTime, MetaData, TypeDecorator, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Explicit naming convention, so an Alembic autogenerate produces the same
# constraint names every time. Without it, SQLite -- which has no constraint
# names of its own -- generates anonymous ones and a later autogenerate against
# PostgreSQL wants to "add" constraints that already exist.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class UtcDateTime(TypeDecorator):
    """`DateTime(timezone=True)` that actually stores and returns aware UTC.

    **This is the fix for a cross-dialect bug that this project hit for real.**

    `DateTime(timezone=True)` is a request to the ORM, not a guarantee from the
    storage engine. PostgreSQL honours it: the column is `timestamptz` and every
    value read back is timezone-aware. SQLite has no timezone concept whatsoever,
    so it silently returns **naive** datetimes -- and the mismatch is invisible
    until something compares a value read from the database with `utcnow()`:

        `expires_at < utcnow()`   ->  TypeError: can't compare offset-naive and
                                     offset-aware datetimes

    That is exactly what the reservation expiry sweeper does, and it is why the
    sweeper appeared to work against PostgreSQL and fail against SQLite -- the
    same code, the same test, one backend lying about the type of its data.

    Coercing in the type decorator fixes every read at once, rather than requiring
    a `.replace(tzinfo=UTC)` at each comparison -- which is a fix that lasts until
    the next query is written. Naive values are assumed UTC, which is correct here
    because `utcnow()` is the only clock that writes them.

    `cache_ok=True` tells SQLAlchemy this is stateless, so it does not have to
    emit a warning per query about un-cacheable types.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        """Normalise on the way in, so what is stored is unambiguous."""
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        """Normalise on the way out, so both dialects return the same type."""
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class Base(DeclarativeBase):
    """Declarative base with the naming convention attached."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampMixin:
    """`created_at` / `updated_at`, both server-defaulted.

    Server-defaulted rather than Python-defaulted so a row inserted by a script,
    a migration, or `psql` still gets a real timestamp. A nullable
    `created_at` on an audit table is an audit table with holes in it.

    Typed `UtcDateTime` rather than `DateTime(timezone=True)`, which means these
    columns come back timezone-aware on **both** backends -- see that class for
    the comparison failure this prevents.
    """

    created_at: Mapped[datetime] = mapped_column(
        UtcDateTime, server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
