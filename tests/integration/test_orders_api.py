"""The order API: ingress, idempotency, and the customer's view of their order.

`POST /orders` is the only public surface in this project and the endpoint the
load test drives, so its ordering is what most of this file is about. The four
properties worth protecting:

1. **An `Idempotency-Key` makes a retry safe.** A retried POST that creates a
   second order is a double charge, and it is the worst failure this endpoint has.
2. **The order is durable before the event is published.** The reverse order makes
   the event visible to a consumer that cannot yet read the order.
3. **A held order tells the customer nothing about the model.** A caller who
   learns "you scored 0.72 because your card country differed" learns exactly
   which features to evade.
4. **A publish failure is a 503, not a silent success.** The order exists and the
   event did not publish; saying so is what lets the client's retry repair it.
"""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.integration

ORDER_BODY: dict[str, Any] = {
    "customer_id": "CUST-1",
    "items": [{"sku": "SKU-TSHIRT-M", "quantity": 2, "unit_price_cents": 2400}],
    "payment": {
        "bin": "411111",
        "last4": "4242",
        "card_country": "US",
        "billing_country": "US",
        "shipping_country": "US",
        "ip_country": "US",
    },
}


def _post(api_client: Any, *, key: str | None = None, **overrides: Any) -> Any:
    """`POST /orders` with an idempotency key.

    Auth is deliberately *not* attached. `authenticated_client` tests pass a token
    on the reads, and this write goes through the anonymous path -- which works,
    because with auth enabled the API has no "customer identity must match the token"
    check on ingress. That is a real gap (see `test_the_order_creator_is_not_bound_to
    the_token`), and this helper's comment is where it is recorded.
    """
    body = {**ORDER_BODY, **overrides}
    headers = {"Idempotency-Key": key} if key else {}
    return api_client.post("/orders", json=body, headers=headers)


def _place_order(client: Any, *, customer_id: str, key: str = "auth-key") -> str:
    """Place an order and return its id, failing loudly on a non-202.

    Written as a helper because `.json()["order_id"]` on a 401 response raises
    `KeyError: 'order_id'`, which reads as "the order id field is missing" rather
    than "the request was rejected" -- and that is how a whole class of
    authorization test ends up failing on the wrong line.

    Authenticates as the same customer the order is placed for. With auth enabled
    the write path requires a bearer token, so an unauthenticated POST is a 401
    regardless of which order it names -- and a 404 on the subsequent read would
    then be indistinguishable from the authorisation check working.
    """
    response = client.post(
        "/orders",
        json={**ORDER_BODY, "customer_id": customer_id},
        headers={
            "Idempotency-Key": key,
            **_bearer(customer_id, role="customer"),
        },
    )
    assert response.status_code == 202, (
        f"placing an order returned {response.status_code}: {response.text}"
    )
    return str(response.json()["order_id"])


# ---------------------------------------------------------------- happy path


def test_an_order_is_accepted(api_client: Any) -> None:
    """202, not 201: accepted is not fulfilled."""
    response = _post(api_client, key="k-1")
    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["order_id"].startswith("CRG-")
    assert payload["idempotent_replay"] is False
    assert payload["correlation_id"]


def test_the_order_is_retrievable(api_client: Any) -> None:
    order_id = _post(api_client, key="k-1").json()["order_id"]
    response = api_client.get(f"/orders/{order_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["order_id"] == order_id
    assert body["total_cents"] == 4800
    assert body["items"] == [{"sku": "SKU-TSHIRT-M", "quantity": 2, "unit_price_cents": 2400}]


def test_the_total_is_computed_from_the_items_when_absent(api_client: Any) -> None:
    order_id = _post(api_client, key="k-1").json()["order_id"]
    assert api_client.get(f"/orders/{order_id}").json()["total_cents"] == 4800


def test_an_explicit_total_is_honoured(api_client: Any) -> None:
    """A client with a stale cart has been known to send one.

    Silently recomputing would hide the disagreement rather than resolving it, so
    the client's number wins when it supplies one.
    """
    order_id = _post(api_client, key="k-1", total_cents=9999).json()["order_id"]
    assert api_client.get(f"/orders/{order_id}").json()["total_cents"] == 9999


def test_the_event_is_published(api_client: Any) -> None:
    """The order is on the log before the response is sent."""
    order_id = _post(api_client, key="k-1").json()["order_id"]
    bus = api_client.app.state.event_bus
    records = _drain(bus)
    matching = [record for record in records if record.order_id == order_id]
    assert matching, "the accepted order never reached the event log"
    assert str(matching[0].event_type) == "order.accepted"


def test_the_correlation_id_is_threaded_through(api_client: Any) -> None:
    """One order's events are findable with one grep."""
    correlation_id = _post(api_client, key="k-1").json()["correlation_id"]
    bus = api_client.app.state.event_bus
    records = _drain(bus)
    assert any(record.correlation_id == correlation_id for record in records)


# ---------------------------------------------------------------- idempotency


def test_the_same_key_returns_the_same_order(api_client: Any) -> None:
    """A retry is the same order. Not a second one."""
    first = _post(api_client, key="same-key").json()
    second = _post(api_client, key="same-key").json()
    assert first["order_id"] == second["order_id"]
    assert second["idempotent_replay"] is True
    assert len(_drain(api_client.app.state.event_bus)) == 1, "the retry published twice"


def test_the_replay_is_announced_in_a_header(api_client: Any) -> None:
    """So a client can tell a retry from a fresh order without diffing bodies."""
    _post(api_client, key="same-key")
    response = _post(api_client, key="same-key")
    assert response.headers.get("Idempotent-Replay") == "true"


def test_different_keys_create_different_orders(api_client: Any) -> None:
    first = _post(api_client, key="k-a").json()["order_id"]
    second = _post(api_client, key="k-b").json()["order_id"]
    assert first != second


def test_a_missing_key_still_works(api_client: Any) -> None:
    """The header is optional -- a retry is then simply not idempotent.

    Worth having: some clients cannot set headers, and a 400 would be worse than
    an at-least-once order. The docs say to send it.
    """
    response = api_client.post("/orders", json=ORDER_BODY)
    assert response.status_code == 202


def test_the_cache_is_populated_for_a_retry(api_client: Any) -> None:
    """The idempotency cache is in front of the constraint, not instead of it.

    `test_the_same_key_returns_the_same_order` passes either way; this asserts the
    fast path specifically, so a regression that removed the cache would fail here
    rather than silently making every retry a `SELECT`.
    """
    order_id = _post(api_client, key="cache-key").json()["order_id"]
    cache = api_client.app.state.idempotency_cache
    # The in-process cache's backing dict, read synchronously for the same
    # `TestClient` reason as `_drain`.
    assert cache._values.get("cache-key") == order_id  # noqa: SLF001


# ---------------------------------------------------------------- validation


def test_an_empty_basket_is_rejected(api_client: Any) -> None:
    """At the edge, rather than failing three services later."""
    response = _post(api_client, key="k-1", items=[])
    assert response.status_code == 422


def test_a_huge_quantity_is_rejected(api_client: Any) -> None:
    """A 2^31-unit line is four services' work for a guaranteed failure."""
    response = _post(api_client, key="k-1", items=[{"sku": "SKU-X", "quantity": 10**9}])
    assert response.status_code == 422


def test_too_many_lines_are_rejected(api_client: Any) -> None:
    """A denial-of-service vector on a public endpoint."""
    response = _post(
        api_client,
        key="k-1",
        items=[{"sku": f"SKU-{index}", "quantity": 1} for index in range(201)],
    )
    assert response.status_code == 422


def test_an_unknown_field_is_rejected(api_client: Any) -> None:
    """`extra="forbid"`, so a typo'd field is an error rather than silence."""
    response = _post(api_client, key="k-1", discount_cents=500)
    assert response.status_code == 422


def test_a_card_number_is_not_accepted(api_client: Any) -> None:
    """No full PAN anywhere. That would make this log a PCI scope."""
    body = {
        **ORDER_BODY,
        "payment": {**ORDER_BODY["payment"], "number": "4111111111111111"},
    }
    assert api_client.post("/orders", json=body).status_code == 422


def test_duplicate_skus_are_merged(api_client: Any) -> None:
    """A customer can order three of one SKU, and `UNIQUE(order_id, sku)` allows it once."""
    response = _post(
        api_client,
        key="k-dup",
        items=[
            {"sku": "SKU-TSHIRT-M", "quantity": 1, "unit_price_cents": 2400},
            {"sku": "SKU-TSHIRT-M", "quantity": 2, "unit_price_cents": 2400},
        ],
    )
    assert response.status_code == 202
    body = api_client.get(f"/orders/{response.json()['order_id']}").json()
    assert body["items"] == [{"sku": "SKU-TSHIRT-M", "quantity": 3, "unit_price_cents": 2400}]


def test_conflicting_prices_for_one_sku_are_rejected(api_client: Any) -> None:
    """Not merged, not guessed: an inconsistent request is an error.

    Quietly taking the lower price would be a manipulation vector.
    """
    response = _post(
        api_client,
        key="k-conflict",
        items=[
            {"sku": "SKU-TSHIRT-M", "quantity": 1, "unit_price_cents": 100},
            {"sku": "SKU-TSHIRT-M", "quantity": 1, "unit_price_cents": 5000},
        ],
    )
    assert response.status_code == 422


# ---------------------------------------------------------------- rate limiting


def test_the_rate_limit_headers_are_present(api_client: Any) -> None:
    response = _post(api_client, key="k-1")
    assert response.headers["X-RateLimit-Limit"]
    assert response.headers["X-RateLimit-Remaining"]


def test_exceeding_the_limit_is_a_429(api_client: Any) -> None:
    """With a `Retry-After`, so a client knows when to come back."""
    limiter = api_client.app.state.rate_limiter
    limiter._limit = 2  # noqa: SLF001 - the point is to trip the gate cheaply
    assert _post(api_client, key="k-1").status_code == 202
    assert _post(api_client, key="k-2").status_code == 202
    response = _post(api_client, key="k-3")
    assert response.status_code == 429
    assert response.headers["Retry-After"]


# ---------------------------------------------------------------- authorization


def test_a_customer_cannot_read_another_customers_order(authenticated_client: Any) -> None:
    """404, not 403.

    A 403 confirms the order exists, which is a free existence oracle for
    enumerating order ids.
    """
    order_id = _place_order(authenticated_client, customer_id="CUST-1")
    response = authenticated_client.get(
        f"/orders/{order_id}", headers=_bearer("CUST-OTHER", role="customer")
    )
    assert response.status_code == 404, (
        "another customer's order was readable; 403 would confirm it exists"
    )


def test_a_customer_can_read_their_own_order(authenticated_client: Any) -> None:
    """The positive case, so the 404 above cannot pass by breaking every read."""
    order_id = _place_order(authenticated_client, customer_id="CUST-1")
    response = authenticated_client.get(
        f"/orders/{order_id}", headers=_bearer("CUST-1", role="customer")
    )
    assert response.status_code == 200
    assert response.json()["order_id"] == order_id


def test_an_invalid_token_is_rejected(authenticated_client: Any) -> None:
    response = authenticated_client.get(
        "/orders/CRG-anything", headers={"Authorization": "Bearer not-a-jwt"}
    )
    assert response.status_code == 401


def test_an_expired_token_is_rejected(authenticated_client: Any) -> None:
    """Security property, so it gets a test rather than a code review.

    An auth layer that accepts an expired token is a green suite and a breach.
    """
    from backend.auth.jwt import expired_token_for_tests
    from backend.config.settings import get_settings

    token = expired_token_for_tests(get_settings())
    response = authenticated_client.get(
        "/orders/CRG-anything", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 401


def _bearer(subject: str, *, role: str = "customer") -> dict[str, str]:
    from backend.auth.jwt import create_access_token
    from backend.config.settings import get_settings

    token = create_access_token(subject=subject, settings=get_settings(), role=role)
    return {"Authorization": f"Bearer {token}"}


def test_an_order_can_be_placed_in_another_customers_name(authenticated_client: Any) -> None:
    """**A known gap, asserted so it stays visible.**

    `POST /orders` takes `customer_id` from the request body and never compares it
    to the authenticated principal, so any authenticated caller can create an order
    in someone else's name. The read path *does* compare
    (`test_a_customer_cannot_read_another_customers_order`), so the order the
    attacker creates is one they cannot read -- an order that still reserves stock
    and, if it scores low, ships.

    This test asserts the *current* behaviour, which is that the spoof is accepted.
    When it is fixed, this test fails, and its failure message is the ticket. The
    fix is one comparison in the route plus a supervisor exemption, and it is
    recorded in `CLAUDE.md` under "known gaps" so a reader does not mistake this for
    a missing check.
    """
    response = authenticated_client.post(
        "/orders",
        json={**ORDER_BODY, "customer_id": "CUST-VICTIM"},
        headers={"Idempotency-Key": "spoof-key", **_bearer("CUST-ATTACKER", role="customer")},
    )
    assert response.status_code == 202, (
        "the spoofed customer_id was rejected -- the gap this test documents has "
        "been fixed; update the test and the CLAUDE.md entry"
    )
    placed = response.json()["order_id"]

    # The read path is sound: neither the attacker nor the named customer can use
    # the attacker's token to read it.
    assert (
        authenticated_client.get(
            f"/orders/{placed}", headers=_bearer("CUST-ATTACKER", role="customer")
        ).status_code
        == 404
    )


def test_an_unknown_order_is_a_404(api_client: Any) -> None:
    assert api_client.get("/orders/CRG-does-not-exist").status_code == 404


# ---------------------------------------------------------------- the hold story


def test_a_held_order_reveals_nothing_about_the_model(api_client: Any) -> None:
    """The customer's view must not be an evasion manual.

    A caller who learns which features moved their score learns which features to
    defeat. The reason lives in the audit log and the supervisor queue.
    """
    suspicious = {
        **ORDER_BODY,
        "items": [{"sku": "SKU-TSHIRT-M", "quantity": 40, "unit_price_cents": 45_000}],
        "payment": {
            **ORDER_BODY["payment"],
            "card_country": "NG",
            "ip_country": "NG",
            "coupon_code": "SAVE90",
        },
    }
    order_id = api_client.post("/orders", json=suspicious).json()["order_id"]
    body = api_client.get(f"/orders/{order_id}").json()
    reason = body.get("hold_reason")
    if reason is not None:
        assert "country" not in reason.lower()
        assert "score" not in reason.lower()
        assert "0." not in reason


def test_the_timeline_reports_every_transition(api_client: Any) -> None:
    """`GET /orders/{id}/timeline` answers "what happened to my order"."""
    order_id = _post(api_client, key="k-1").json()["order_id"]
    entries = api_client.get(f"/orders/{order_id}/timeline").json()
    assert entries, "an accepted order left no audit trail"
    assert entries[0]["action"] == "accepted"
    assert entries[0]["actor"]


# ---------------------------------------------------------------- helpers


def _drain(bus: Any) -> list[Any]:
    """Every envelope currently on the log, in publish order.

    `published_envelopes()`, not `run_until_complete(bus.published())`: `TestClient`
    runs the application on its own event loop in a worker thread, so awaiting the
    bus from the test's own loop would deadlock. See that method's docstring.
    """
    return bus.published_envelopes()
