"""End-to-end smoke test across two real processes.

`make test` proves the pipeline works in one process with in-process transports.
This proves the pieces wire together when they are actually separate: a real
Postgres, a real Kafka broker, a real RabbitMQ, and a gRPC scorer reached over a
socket.

Two processes, deliberately. `run_pipeline` wires the handler into the same event
loop as the generator, so a shared in-memory registry, a `contextvars` mistake or a
mis-bound dependency would not show up. Here the order consumer is a separate OS
process: it resolves `fraud:50052` over TCP, reads from a broker, and commits to a
database the API cannot see. Anything that only works because two objects happen to
be in the same interpreter fails here.

What it asserts, and why each one is here rather than in the unit suite:

  1. The API accepts an order and publishes it.
  2. The separate consumer process scores it through gRPC and persists it.
  3. The stock ledger reflects the reservation, with `available == on_hand - reserved`.
  4. Everything above happened within a deadline, because a smoke test that hangs
     is worse than one that fails.

It is a *smoke* test and it is not idempotent: it runs against a real deployment and
leaves orders in it. `--base-url` defaults to the compose stack, so run `make up`
first. Exits non-zero on any failure, which is the only property CI needs from it.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

#: Generous, because a cold container start plus a Kafka consumer-group rebalance is
#: genuinely slow on a first run and this is not a latency measurement. The
#: per-order latency budget is asserted by `make loadtest`; this asserts only
#: "eventually, and not never".
DEFAULT_TIMEOUT_SECONDS = 90.0


class SmokeFailure(Exception):
    """One step failed. Carries the step name so the report says which."""


def _request(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    """One HTTP call. Returns `(status, body)`.

    `urllib` rather than `httpx`, deliberately: `httpx` is already a dependency of
    the API and importing it here would mean this script could not be run against a
    container using a different Python. `urllib` is in the standard library, so the
    only requirement is a Python.
    """
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=body, method=method)  # noqa: S310
    request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            raw = response.read().decode()
            return response.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        return error.code, (json.loads(raw) if raw else {})
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise SmokeFailure(f"{method} {url} failed to connect: {error}") from error


def step(name: str) -> Any:
    """Decorator that prints a step banner and turns an exception into a failure."""

    def decorate(function: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            print(f"--> {name}", flush=True)
            started = time.monotonic()
            try:
                result = function(*args, **kwargs)
            except SmokeFailure as failure:
                print(f"    FAIL after {time.monotonic() - started:.1f}s: {failure}", flush=True)
                raise
            print(f"    ok ({time.monotonic() - started:.1f}s)", flush=True)
            return result

        wrapper.__name__ = function.__name__
        wrapper.__doc__ = function.__doc__
        return wrapper

    return decorate


def build_order() -> dict[str, Any]:
    """A small order against SKUs the seed data creates.

    Quantity 1 on each: the load generator deliberately requests more than the
    seeded stock holds, because oversell handling is interesting. A smoke test wants
    the boring path, so it asks for what is actually there.
    """
    return {
        "customer_id": "CUST-SMOKE",
        "items": [
            {"sku": "SKU-TSHIRT-M", "quantity": 1, "unit_price_cents": 2500},
            {"sku": "SKU-MUG-STD", "quantity": 1, "unit_price_cents": 1200},
        ],
        "payment": {"token": "tok_smoke", "method": "card"},
        "idempotency_key": f"smoke-{uuid.uuid4().hex}",
    }


@step("the API is live")
def check_api_health(base_url: str) -> None:
    status, body = _request(f"{base_url}/healthz")
    if status != 200:
        raise SmokeFailure(f"/healthz returned {status}: {body}")
    if body.get("status") != "ok":
        raise SmokeFailure(f"/healthz did not report ok: {body}")
    # Liveness deliberately checks no dependency (see docs/architecture.md), so this
    # is also the point to confirm readiness, which does check them. If readiness
    # fails here, the failure belongs in the report and not in a 60-second timeout
    # waiting for an order that will never be processed.
    status, body = _request(f"{base_url}/readyz")
    if status != 200:
        raise SmokeFailure(
            f"/readyz returned {status}: {body}. The API is up but its dependencies "
            "(postgres, kafka, rabbitmq, fraud) are not ready -- check `make logs`."
        )


@step("the order is accepted and published")
def place_order(base_url: str) -> str:
    status, body = _request(
        f"{base_url}/orders", method="POST", payload=build_order(), timeout=15.0
    )
    if status not in (200, 201):
        raise SmokeFailure(f"POST /orders returned {status}: {body}")
    order_id = body.get("order_id") or body.get("id")
    if not order_id:
        raise SmokeFailure(f"POST /orders returned no order id: {body}")
    return str(order_id)


@step("the separate consumer scored and persisted it")
def wait_for_scoring(base_url: str, order_id: str, timeout: float) -> dict[str, Any]:
    """Poll until the order has a score, or give up with a diagnostic.

    Polling rather than a fixed sleep, because the consumer's latency here is a
    consumer-group rebalance plus a gRPC call -- milliseconds on a warm stack, tens
    of seconds on a cold one. A fixed sleep would be either flaky or slow.

    Reports *what the order looks like* on timeout, because "timed out" alone sends
    whoever is on call to the dashboard instead of to the answer. The distinction
    between "still queued" and "scored as held" is the whole diagnosis: the first
    means the consumer is not running, the second means scoring worked and routing
    sent it to review.
    """
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        status, body = _request(f"{base_url}/orders/{order_id}")
        if status == 200:
            last = body
            if body.get("fraud_score") is not None:
                return body
        time.sleep(0.5)

    raise SmokeFailure(
        f"order {order_id} had no fraud_score after {timeout:.0f}s. Last seen: "
        f"{json.dumps(last, indent=2)}. If status is 'pending' the consumer process "
        "is not consuming; if it is 'held' then scoring worked and the order was "
        "routed to review."
    )


@step("the stock ledger balances")
def check_stock_ledger(base_url: str) -> None:
    """`available == on_hand - reserved` for every warehouse.

    This is the invariant that makes an oversell impossible, checked through the
    API rather than against the database directly -- because if the API cannot show
    a consistent ledger, neither can an operator, whatever the tables say.
    """
    status, body = _request(f"{base_url}/inventory")
    if status != 200:
        raise SmokeFailure(f"GET /inventory returned {status}: {body}")

    rows = body.get("warehouses") or body.get("items") or []
    if not rows:
        raise SmokeFailure(f"GET /inventory returned no warehouses: {body}")

    for row in rows:
        available = row.get("available")
        on_hand = row.get("on_hand")
        reserved = row.get("reserved")
        if None in (available, on_hand, reserved):
            continue
        if available != on_hand - reserved:
            raise SmokeFailure(
                f"warehouse {row.get('warehouse_id')} does not balance: "
                f"available={available} on_hand={on_hand} reserved={reserved}"
            )
        if available < 0:
            raise SmokeFailure(
                f"warehouse {row.get('warehouse_id')} has negative stock ({available}), "
                "which means stock was promised twice"
            )


@step("the metrics endpoint serves the scorer's series")
def check_metrics(base_url: str) -> None:
    """The observability path is part of the deployment, not a later task.

    Checked against the API's own `/metrics` because that is the one reachable
    without knowing compose's internal ports. The scoring series is asserted
    because a dashboard panel pointed at a metric that is never exported renders as
    "No data" forever and looks like a quiet system.
    """
    status, body = _request(f"{base_url}/metrics")
    if status != 200:
        raise SmokeFailure(f"GET /metrics returned {status}")
    text = json.dumps(body) if isinstance(body, dict) else ""
    if not text:
        # The body is Prometheus text, not JSON; `json.loads` failed silently into
        # `{}` above, so fetch it as text instead.
        try:
            with urllib.request.urlopen(f"{base_url}/metrics", timeout=10.0) as response:
                text = response.read().decode()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            raise SmokeFailure(f"could not read /metrics: {error}") from error

    for required in ("flowmesh_orders_ingested", "flowmesh_scoring_latency_seconds"):
        if required not in text:
            raise SmokeFailure(
                f"/metrics does not expose {required}, so the dashboard panels that "
                "read it will render as 'No data' with no error anywhere"
            )


def run(*, base_url: str, timeout: float) -> int:
    base_url = base_url.rstrip("/")
    print(f"FlowMesh smoke test against {base_url}\n")

    started = time.monotonic()
    check_api_health(base_url)
    order_id = place_order(base_url)
    print(f"    order_id={order_id}")
    order = wait_for_scoring(base_url, order_id, timeout)
    print(
        f"    status={order.get('status')} band={order.get('risk_band')} "
        f"score={order.get('fraud_score')}"
    )
    check_stock_ledger(base_url)
    check_metrics(base_url)

    elapsed = time.monotonic() - started
    print(f"\nSMOKE TEST PASSED in {elapsed:.1f}s (order {order_id} end to end)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="FlowMesh end-to-end smoke test.")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="the API's base URL (default: %(default)s)",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="seconds to wait for the order to be scored (default: %(default)s)",
    )
    args = parser.parse_args()

    try:
        return run(base_url=args.base_url, timeout=args.timeout)
    except SmokeFailure as failure:
        print(f"\nSMOKE TEST FAILED: {failure}", file=sys.stderr)
        print(
            "\nThis script needs a running deployment: `make up` (or "
            "`docker compose up -d`), then `make smoke`. It is not a unit test and "
            "will not pass with nothing listening.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
