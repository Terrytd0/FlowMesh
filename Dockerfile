# FlowMesh — development and CI image.
#
# Multi-stage because the runtime image needs none of the build tooling: no
# compiler toolchain, no grpcio-tools, no pip. A single-stage image here would
# ship a compiler and the protobuf generator to production for no benefit.
#
# python:3.13-slim rather than alpine: aiosqlite, asyncpg and grpcio all ship
# musl wheels that are less consistently available, and a build that occasionally
# falls back to compiling from source is worse than an image 60MB larger.
#
# The uv version is pinned to the one that generated `uv.lock`. A mismatch is not
# cosmetic: uv rewrites the lockfile's format when it disagrees, and `--frozen` then
# fails -- so an unpinned `latest` makes the build depend on when it ran.

# ---------------------------------------------------------------- builder
FROM python:3.13-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies come from `requirements.txt`, a pinned export of `uv.lock`.
#
# `pip install .` does not work, and it failed twice with two errors that look
# unrelated: `No matching distribution found for grpcio>=1.68.0 (from versions: none)`,
# then `ResolutionImpossible` after several minutes of backtracking. Both are the same
# problem -- pip resolving 87 packages from a live index, with nothing pinning
# grpcio, grpcio-status and protobuf to versions that agree with each other. Those
# three have to move together and pip backtracks through combinations until one fits.
#
# The export pins every one of them, so pip installs rather than solves. A dependency
# conflict becomes a lockfile diff to review instead of a build that fails at minute
# four. Regenerate with `uv export --format requirements-txt --no-hashes
# --no-emit-project --frozen -o requirements.txt`.
#
# No uv in the image. It was tried first, copying the static binary from
# ghcr.io, and the 22MB layer truncated twice at ~10MB with `unexpected EOF` after
# ~15 minutes. One more network dependency in a build that already needs the network
# is not worth a resolver's features when pip plus a pinned file does the job.
COPY requirements.txt ./

# `--retries` is not paranoia about a bad network, it is the difference between a
# build that works and one that does not. pip surfaces a truncated or dropped
# response to an index query as `No matching distribution found for yarl==1.25.1
# (from versions: none)` -- indistinguishable from the package genuinely not
# existing, and it looks like a requirements bug. This build hit that for grpcio and
# then for yarl on a network where a 22MB layer had already truncated twice. Retrying
# the query is the correct response and pip does it by default, at 5; 10 with a longer
# timeout covers the slow-link case where five attempts all timed out.
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --retries 10 --timeout 120 \
        -r requirements.txt

# The project itself is installed *after* the source is copied, and that ordering is
# load-bearing: hatchling reads `[tool.hatch.build.targets.wheel] packages =
# ["backend"]` and packages the directory named there, so with only pyproject.toml
# present it fails with "Unable to determine which files to ship inside the wheel" --
# an error that reads like a packaging bug rather than a missing COPY.
#
# Not `pip install -e .`. An editable install puts a `.pth` file pointing at /app into
# site-packages, so the image depends on the build directory staying put and is no
# longer a copy of anything.
COPY pyproject.toml README.md ./
COPY backend/ ./backend/
COPY proto/ ./proto/
RUN /opt/venv/bin/pip install --no-cache-dir --no-deps .

# ---------------------------------------------------------------- runtime
FROM python:3.13-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

# curl for the container healthcheck. Without it the healthcheck would have to be
# a Python one-liner, which is slower and less obvious in `docker ps`.
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app

# Deliberately NOT copying `backend/`.
#
# It looks like the source has to be here, but the package is already installed into
# /opt/venv by the builder, and copying it as well puts a second copy of every module
# on `sys.path` -- `/app` (the WORKDIR) takes precedence over site-packages. That is a
# genuine hazard rather than a cosmetic duplication: `backend.database.base.Base` is a
# module-level object, so a module imported as `backend.database.base` from one copy
# and the same name from the other produces two distinct `Base` classes, two distinct
# `MetaData`, and `create_all()` on one silently creates no tables for the other. The
# symptom -- `no such table: orders` -- names neither cause.
#
# So the installed copy is the only copy. What is copied here is what the install
# cannot provide: Alembic (not part of the wheel), the model weights (runtime data),
# and `scripts/` (`make smoke`).
COPY alembic.ini ./
COPY alembic/ ./alembic/
COPY data/model/ ./data/model/
COPY scripts/ ./scripts/

# Run unprivileged. The API needs no write access outside its own data directory,
# and a container that runs as root turns any future path-traversal bug into a
# container escape.
RUN useradd --create-home --shell /usr/bin/bash flowmesh \
    && mkdir -p /app/data/runtime \
    && chown -R flowmesh:flowmesh /app
USER flowmesh

EXPOSE 8000 50052 50055

# The API's liveness endpoint, which deliberately checks no dependency -- see
# docs/architecture.md. A healthcheck that touched the database would mark the API
# unhealthy during a Postgres restart, and `depends_on` would then cascade.
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]