"""The worker metrics endpoint, over a real socket.

The claim being tested is that these bytes are valid HTTP — status line, headers,
`Content-Length` matching the body, and the exposition format Prometheus actually
parses. Calling `_build_response` directly would assert that the function returns
the bytes it is supposed to return, which is the weaker and easier claim.

`port=0` so the OS picks a free port: a test that binds 50052 fails on a
developer who already has a worker running, and a suite that cannot run in
parallel is a suite people run less.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from backend.observability.serve import METRICS_PATH, serve_metrics


@pytest.fixture
async def server(metrics: Any) -> AsyncIterator[Any]:
    """A running metrics server on an ephemeral port."""
    running = await serve_metrics(metrics, port=0, host="127.0.0.1")
    async with running:
        yield running


async def _get(port: int, path: str = METRICS_PATH) -> tuple[str, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(
            f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=5.0)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass
    header, _, body = raw.partition(b"\r\n\r\n")
    return header.decode("latin-1"), body


def _port(server: Any) -> int:
    sockets = server.sockets or []
    assert sockets, "the server has no socket"
    return int(sockets[0].getsockname()[1])


async def test_metrics_are_served_over_a_real_socket(server: Any, metrics: Any) -> None:
    metrics.orders_ingested.labels(outcome="approved").inc(7)
    header, body = await _get(_port(server))
    assert "200 OK" in header
    assert "flowmesh_orders_ingested_total" in body.decode()
    assert 'outcome="approved"' in body.decode()


async def test_the_exposition_is_stable_across_two_scrapes(server: Any) -> None:
    """Two scrapes of an unchanged registry are byte-identical.

    Not cosmetic: a `_created` timestamp that changes per scrape would make every
    panel's data different on every refresh, and a stable exposition is what lets
    a scrape be compared against the previous one.
    """
    _header, first = await _get(_port(server))
    _header2, second = await _get(_port(server))
    assert first == second, "two scrapes of an unchanged registry differ"


async def test_the_declared_length_equals_the_body(server: Any) -> None:
    header, body = await _get(_port(server))
    declared = None
    for line in header.split("\r\n"):
        if line.lower().startswith("content-length:"):
            declared = int(line.split(":", 1)[1].strip())
    assert declared == len(body), (
        f"Content-Length says {declared} but the body is {len(body)} bytes"
    )


async def test_the_exposition_format_is_the_prometheus_one(server: Any) -> None:
    header, body = await _get(_port(server))
    assert "text/plain; version=0.0.4" in header
    text = body.decode()
    # A sample line, exactly as Prometheus expects it.
    assert any(
        line.startswith("# HELP ") and line.endswith(" ") is False
        for line in text.splitlines()
        if line.startswith("# HELP flowmesh_orders_ingested_total")
    ), "no HELP line for a known metric -- the exposition is malformed"
    assert "# TYPE flowmesh_orders_ingested_total counter" in text


async def test_healthz_is_liveness_only(server: Any) -> None:
    """No dependency is touched, which is the entire point.

    A metrics/health server that checked the database would return 500 during a
    Postgres restart, and a container healthcheck failing on that takes the service
    out of rotation for something that is still perfectly healthy.
    """
    header, body = await _get(_port(server), "/healthz")
    assert "200 OK" in header
    assert b"/metrics" in body


async def test_the_root_path_is_not_a_404(server: Any) -> None:
    """A bare `/` returning 404 reads as "this service is broken"."""
    header, _ = await _get(_port(server), "/")
    assert "200 OK" in header


async def test_an_unknown_path_is_a_404(server: Any) -> None:
    header, _ = await _get(_port(server), "/admin/secrets")
    assert "404" in header


async def test_a_query_string_does_not_break_routing(server: Any) -> None:
    """Prometheus does not send one, but a human curling the endpoint will."""
    header, body = await _get(_port(server), f"{METRICS_PATH}?foo=bar")
    assert "200 OK" in header
    assert body


async def test_a_scrape_after_a_metric_change_reflects_it(server: Any, metrics: Any) -> None:
    """The server renders live state, not a cached first scrape."""
    _header, before = await _get(_port(server))
    metrics.orders_ingested.labels(outcome="approved").inc(1)
    _header2, after = await _get(_port(server))
    assert before != after, "a metric change did not appear in the exposition"
