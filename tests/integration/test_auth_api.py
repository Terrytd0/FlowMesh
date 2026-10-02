"""`POST /auth/login` -- the one route that turns a password into a token.

This endpoint was broken for the entire life of the project and no test called it.
Every other authorisation test in the suite mints its own token with
`create_access_token`, so the only route that a real client has to use first was the
only one with no coverage. It answered *every* request with

    422 {"detail": [{"type": "missing", "loc": ["query", "body"]},
                    {"type": "missing", "loc": ["query", "auth"]}]}

which is what FastAPI says when it cannot resolve a handler's annotations and has
quietly demoted them to query parameters. `README.md`'s quick start begins with
`POST /auth/login`, so the documented first step of the project did not work.
"""

from __future__ import annotations

from typing import Any

import pytest

CREDENTIALS = [
    ("customer@example.com", "customer-pass", "customer"),
    ("supervisor@example.com", "supervisor-pass", "supervisor"),
    ("admin@example.com", "admin-pass", "admin"),
]


@pytest.mark.parametrize(("email", "password", "role"), CREDENTIALS)
def test_login_returns_a_token_for_a_seeded_user(
    authenticated_client: Any, email: str, password: str, role: str
) -> None:
    """The documented flow works, for every seeded role."""
    response = authenticated_client.post("/auth/login", json={"email": email, "password": password})

    assert response.status_code == 200, (
        f"{email} could not log in: {response.status_code} {response.text}"
    )
    body = response.json()
    assert body["access_token"], "a 200 with no access_token is not a login"
    assert body["token_type"].lower() == "bearer"
    assert body["role"] == role, f"expected {role}, got {body['role']}"


def test_a_wrong_password_is_refused_without_revealing_which_part_was_wrong(
    authenticated_client: Any,
) -> None:
    """One message and one status for a bad user and a bad password.

    Distinguishing "no such user" from "wrong password" is account enumeration, and
    the cost of getting it wrong is a credential-stuffing oracle that costs an
    attacker nothing to run.
    """
    bad_password = authenticated_client.post(
        "/auth/login", json={"email": "customer@example.com", "password": "wrong"}
    )
    no_such_user = authenticated_client.post(
        "/auth/login", json={"email": "nobody@example.com", "password": "customer-pass"}
    )

    assert bad_password.status_code == 401
    assert no_such_user.status_code == 401
    assert bad_password.json() == no_such_user.json(), (
        "the two failures are distinguishable, which is account enumeration"
    )


def test_a_login_body_is_a_body_not_a_query_string(authenticated_client: Any) -> None:
    """The regression, stated as a contract on the schema rather than the response.

    Asserting "login works" catches today's break. Asserting that the parameter is
    *in the body* catches the whole class: FastAPI resolves a handler's annotations
    lazily and, when it cannot, demotes an unresolvable model parameter to a query
    parameter instead of raising. The symptom is a 422 that mentions `["query",
    "body"]`, which reads like a malformed request and is really a wiring fault.

    The check walks the OpenAPI schema, so it fails at the point of the mistake
    rather than at the point where somebody tries to use the API.
    """
    schema = authenticated_client.get("/openapi.json").json()
    login = schema["paths"]["/auth/login"]["post"]

    body_schema = next(
        (name for name in login.get("requestBody", {}).get("content", {}) if "json" in name),
        None,
    )
    assert body_schema is not None, (
        "/auth/login declares no JSON request body; the credentials were demoted to "
        f"query parameters. Parameters: "
        f"{[p.get('name') for p in login.get('parameters', [])]}"
    )
    assert not login.get("parameters"), (
        "POST /auth/login takes query parameters, so it cannot receive a JSON body: "
        f"{[p.get('name') for p in login['parameters']]}"
    )
