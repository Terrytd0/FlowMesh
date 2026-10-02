"""Application logging.

Idempotent `configure_logging()` called once at process start by every entry
point (`backend.main`, the gRPC fraud server, each consumer, each worker).

Identifiers only at INFO: never full card numbers, never customer addresses,
never raw order payloads. An event log is exactly what an attacker would want
from a pipeline that processes payment data, and the full record already lives
in Postgres where it is queryable by an operator who has a reason to look.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Final

_CONFIGURED: bool = False
_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s :: %(message)s"
_JSON_FORMAT: Final[str] = (
    '{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}'
)
_DATEFMT: Final[str] = "%Y-%m-%dT%H:%M:%S%z"


def configure_logging(level: str = "INFO", *, json_output: bool = False) -> None:
    """Install a single stdout handler at `level`. Safe to call repeatedly.

    Idempotent because every entrypoint calls it defensively (FastAPI's lifespan,
    the gRPC server, each consumer) and a second handler would double every log
    line.

    `json_output` exists because a container log that Prometheus or Loki has to
    regex apart is a log nobody queries. The text format stays the default
    because it is what a human reading `docker compose logs` wants.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter(fmt=_JSON_FORMAT if json_output else _FORMAT, datefmt=_DATEFMT)
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

    # These are chatty at DEBUG and drown out our own output during a load test.
    logging.getLogger("grpc").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("aio_pika").setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a module-scoped logger. Use this instead of `print`."""
    return logging.getLogger(name)


def log_extra(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Log `message` with structured key/value context appended.

    Values are stringified here rather than passed through, because a single
    non-serialisable value in a JSON log sink takes down the log call, and a log
    call must never be able to fail the operation it is describing.
    """
    suffix = " ".join(f"{key}={value!s}" for key, value in fields.items() if value is not None)
    logger.log(level, f"{message} {suffix}".rstrip())


def dumps(value: Any) -> str:
    """Compact JSON for log lines, never raising on unserialisable input."""
    try:
        return json.dumps(value, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return json.dumps(str(value))
