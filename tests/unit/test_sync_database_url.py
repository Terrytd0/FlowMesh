"""The synchronous DSN Alembic runs on, derived from the configured one.

This property has exactly one caller -- `alembic/env.py` -- and the default
database is SQLite. That combination is what let it be wrong for its whole life
without anything noticing: it stripped `+asyncpg` and returned a bare
`postgresql://`, which SQLAlchemy 2.1 resolves to psycopg (v3). The project
depends on psycopg2, so the first thing that touched a real PostgreSQL was
`alembic upgrade head` in CI, which failed with `ModuleNotFoundError: No module
named 'psycopg'`.

The assertions below are therefore about the *driver*, not the string: a
substring check would pass on the broken `postgresql://` too, since "postgresql"
is a substring of both. Resolving the dialect is what actually catches it.
"""

from __future__ import annotations

import pytest

from backend.config.settings import Settings


@pytest.mark.parametrize(
    ("configured", "expected_scheme"),
    [
        ("postgresql+asyncpg://u:p@localhost:5432/db", "postgresql+psycopg2"),
        ("postgresql://u:p@localhost:5432/db", "postgresql+psycopg2"),
        ("postgresql+psycopg2://u:p@localhost:5432/db", "postgresql+psycopg2"),
        ("sqlite+aiosqlite:///./data.db", "sqlite"),
        ("sqlite:///./data.db", "sqlite"),
    ],
)
def test_the_derived_sync_url_names_a_driver_this_project_installs(
    configured: str, expected_scheme: str
) -> None:
    settings = Settings(database_url=configured)

    derived = settings.effective_database_url_sync

    scheme, _, _ = derived.partition("://")
    assert scheme == expected_scheme


def test_a_postgres_sync_url_resolves_to_an_installed_dbapi() -> None:
    """The regression itself, checked by resolving rather than by string match.

    `create_engine` is what Alembic calls, so this asserts on the thing that
    actually broke: a bare `postgresql://` selects psycopg v3 and raises
    `ModuleNotFoundError` at construction time, whereas `postgresql+psycopg2://`
    selects the driver this project actually depends on.
    """
    from sqlalchemy import create_engine

    settings = Settings(database_url="postgresql+asyncpg://u:p@localhost:5432/db")

    engine = create_engine(settings.effective_database_url_sync)

    assert engine.dialect.driver == "psycopg2"


def test_an_explicit_sync_url_wins_over_the_derived_one() -> None:
    """`database_url_sync` is the escape hatch, and it must not be overwritten."""
    settings = Settings(
        database_url="postgresql+asyncpg://u:p@localhost:5432/db",
        database_url_sync="sqlite:///./explicit.db",
    )

    assert settings.effective_database_url_sync == "sqlite:///./explicit.db"


def test_the_credentials_and_path_survive_the_derivation() -> None:
    """Only the scheme may change. Dropping the password produces a confusing
    authentication failure rather than an obvious one."""
    settings = Settings(
        database_url="postgresql+asyncpg://user:p%40ss@db.example:5432/app?ssl=require"
    )

    derived = settings.effective_database_url_sync

    assert derived == "postgresql+psycopg2://user:p%40ss@db.example:5432/app?ssl=require"
