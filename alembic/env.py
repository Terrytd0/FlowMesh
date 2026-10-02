"""Alembic environment for FlowMesh.

Two things in here are not boilerplate, and both exist because the obvious version
is wrong.

**The URL comes from `Settings`, not from `alembic.ini`.** A migration run against
a different database from the application is the kind of mistake that leaves a
staging schema three revisions behind and nobody notices until a deploy fails. One
source for the DSN means a migration cannot target the wrong database by
construction.

**`render_as_batch` is on for SQLite.** `alembic check` runs in CI, and the load
test, the chaos test and most of the unit suite run on SQLite. SQLite cannot
`ALTER TABLE ... ALTER COLUMN`, so without batch mode every migration that touches
a column fails on the database most of the tests actually use, and the migration
passes review because it was only ever run against Postgres.
"""

from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool

from alembic import context

# The repo root, so `import backend` resolves no matter where alembic was invoked
# from. `alembic.ini`'s `prepend_sys_path` normally handles this; doing it here too
# makes `env.py` importable from a test or a script that imports it directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Imports the models module for its side effect: it registers every table on
# `Base.metadata`. Importing `backend.database.base` alone leaves the metadata
# empty, `compare_type` finds no differences, and `alembic check` reports "no new
# upgrade operations detected" for a schema that is entirely missing. That silent
# pass is worse than an import error, so this import is explicit and commented.
import backend.database.models  # noqa: E402,F401
from backend.config.settings import get_settings  # noqa: E402
from backend.database.base import Base  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.effective_database_url_sync)

target_metadata = Base.metadata


def _include_object(
    obj: object, name: str | None, type_: str, reflected: bool, compare_to: object
) -> bool:
    """Skip objects Alembic has no business managing.

    `alembic check` compares reflected database objects against model metadata. On
    a database that already holds extensions or tables FlowMesh did not create --
    a managed Postgres with `pg_stat_statements`, an audit trigger's own table --
    autogenerate proposes dropping all of them. Without this filter, the first
    person to run `alembic revision --autogenerate` against a shared database gets
    a migration that drops the extension, and the CI drift check that was supposed
    to protect them generates the same file.
    """
    if type_ == "table" and name in {"spatial_ref_sys", "pg_stat_statements"}:
        return False
    return True


def run_migrations_offline() -> None:
    """Emit SQL without a database connection.

    Used by `alembic upgrade head --sql` to hand a DBA the exact statements. The
    literal URL is rendered with credentials intact, so the output goes to a file
    rather than a terminal in anything shared.
    """
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        include_object=_include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live connection.

    `pool.NullPool` rather than the default pool: a migration script is a
    one-shot process, so a connection pool would be built, used once and discarded
    -- and, worse, `pool_pre_ping` on a pool that is about to be torn down can
    mask a server-side restart by silently reconnecting halfway through a
    `DROP INDEX`.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_object=_include_object,
            # SQLite only. Postgres alters columns in place; SQLite has no
            # `ALTER COLUMN` at all, so batch mode recreates the table. Only set
            # it when needed, since on Postgres batch mode changes nothing useful
            # and is slower.
            render_as_batch=connection.dialect.name == "sqlite",
        )

        with context.begin_transaction():
            context.run_migrations()

    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
