"""The worker metrics endpoint: a real socket, and the reasons it is so small.

A worker has no web framework, which is correct — it has no HTTP API. But a
`FlowMeshMetrics` object nothing scrapes is a counter nobody reads, and "add
Prometheus to the consumer processes" should not mean "give each of them a web
framework".

So this is a `asyncio.start_server` serving exactly one route. Three properties
are the point:

1. **It touches nothing else.** No database, no broker, no LLM. A scrape timeout is
   then unambiguous: it means this process is wedged, not that some dependency is.
2. **It renders from the same registry the API serves.** `generate_latest` is the
   identical call, so a worker's panels and the API's cannot drift in format.
3. **It logs nothing per request.** At a 5s scrape interval that is a line every
   five seconds per service, burying the redelivery and error lines that actually
   need reading.

The boundary test uses a real socket rather than calling `_build_response`
directly, because "the header is well-formed" is exactly the claim a hand-rolled
HTTP server gets wrong, and calling the function under test would assert nothing
about the bytes on the wire.
"""

from __future__ import annotations

import asyncio
from typing import Any

from prometheus_client import CollectorRegistry

from backend.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_METRICS_PORT = 50052
#: The path. `/metrics` for consistency with the API, so one dashboard query works
#: against every target in the compose file.
METRICS_PATH = "/metrics"

#: The 200 response header template. A `str`, not `bytes`: it carries a
#: `{length}` placeholder, so formatting has to happen before encoding.
_RESPONSE_TEMPLATE = (
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
    "Content-Length: {length}\r\n"
    "Connection: close\r\n"
    "\r\n"
)

_NOT_FOUND = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"

_BODY = b"<html><body>FlowMesh metrics: see /metrics</body></html>"


def _build_response(path: str, registry: CollectorRegistry) -> bytes:
    """The whole HTTP response for one path.

    Takes a registry rather than a `FlowMeshMetrics`, so it can be tested against a
    bare `CollectorRegistry` — the rendering is the only thing that matters here.
    """
    from prometheus_client import generate_latest

    if path == METRICS_PATH:
        body = generate_latest(registry)
        return _RESPONSE_TEMPLATE.format(length=len(body)).encode("ascii") + body
    if path in ("/", "/healthz"):
        # `/healthz` here is process liveness, matching the API's. It deliberately
        # checks no dependency -- the process being alive is the whole question, and
        # a probe that failed on a database restart would get the process killed.
        return _RESPONSE_TEMPLATE.format(length=len(_BODY)).encode("ascii") + _BODY
    return _NOT_FOUND


async def _handle(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, registry: CollectorRegistry
) -> None:
    try:
        request = await asyncio.wait_for(reader.readline(), timeout=2.0)
        parts = request.decode("latin-1", errors="replace").split()
        path = parts[1].split("?", 1)[0] if len(parts) >= 2 else "/"
        # Drain the rest of the request so the client sees a clean close rather
        # than a reset while it is still writing headers.
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=0.5)
            if line in (b"\r\n", b"\n", b""):
                break
        writer.write(_build_response(path, registry))
        await writer.drain()
    except (TimeoutError, asyncio.IncompleteReadError, ConnectionError):
        # A scrape that goes away mid-request is normal during a restart.
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass


async def serve_metrics(
    metrics: Any, *, port: int = DEFAULT_METRICS_PORT, host: str = "0.0.0.0"
) -> asyncio.AbstractServer:
    """Start the metrics server and return it. The caller owns the shutdown.

    `metrics` is typed loosely because callers pass a `FlowMeshMetrics` and tests
    pass a `CollectorRegistry` — both have a `.registry`.
    """
    registry: CollectorRegistry = metrics.registry

    async def _on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _handle(reader, writer, registry)

    server = await asyncio.start_server(_on_client, host, port)
    logger.info("metrics server listening on %s:%s%s", host, port, METRICS_PATH)
    return server
