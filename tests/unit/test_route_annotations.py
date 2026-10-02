"""Every route's annotations must actually resolve.

`backend/main.py` uses `from __future__ import annotations`, so every annotation on
every route handler is a **string**. FastAPI evaluates those strings against the
defining module's globals. When a name is not there, it does not raise -- it treats
the parameter as a query parameter and the endpoint quietly changes shape.

That is how `POST /auth/login` came to accept nobody: its handler, its
`Depends(get_auth)` target and its `LoginRequest`/`Annotated` names were all defined
inside a factory function, so none of them were module globals. Every request got a
422 mentioning `["query", "body"]`, and the OpenAPI schema documented query
parameters. The whole feature was dead and nothing raised.

One test caught it by calling the endpoint. This one catches the *class*, over every
route, and it is the reason to have it: the next person who writes a route inside a
function gets a test failure instead of a broken endpoint.
"""

from __future__ import annotations

import inspect
import typing
from typing import Any

#: Routes FastAPI registers itself, whose handlers are deliberately defined inside
#: `FastAPI.setup`'s scope. They are not this project's code, they are never given a
#: body, and asserting anything about them would be asserting about the framework.
_FRAMEWORK_ROUTES = frozenset({"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"})


def _routes(app: Any) -> list[tuple[str, Any]]:
    """`(method, endpoint)` for every route this project defines.

    Included routers are not necessarily flattened into `app.routes` -- recent
    FastAPI keeps them wrapped in a private `_IncludedRouter` reachable through
    `include_context.included_router` -- so this walks the router tree rather than
    assuming a shape. A test that asserted on `app.routes` alone would see four
    documentation routes and pass while every real endpoint was invisible to it,
    which is why `test_the_router_tree_walk_actually_finds_the_endpoints` exists.
    """
    found: list[tuple[str, Any]] = []
    seen: set[int] = set()

    def nested_routes(route: Any) -> list[Any]:
        children = getattr(route, "routes", None)
        if children:
            return list(children)
        context = getattr(route, "include_context", None)
        inner = getattr(context, "included_router", None)
        return list(getattr(inner, "routes", None) or []) if inner is not None else []

    def walk(routes: Any) -> None:
        for route in routes:
            if id(route) in seen:
                continue
            seen.add(id(route))
            path = getattr(route, "path", None)
            endpoint = getattr(route, "endpoint", None)
            if endpoint is not None and path not in _FRAMEWORK_ROUTES:
                methods = ",".join(sorted(getattr(route, "methods", None) or ["-"]))
                found.append((f"{methods} {path}", endpoint))
            children = nested_routes(route)
            if children:
                walk(children)

    walk(app.routes)
    return found


def test_the_router_tree_walk_actually_finds_the_endpoints() -> None:
    """Guard the guard.

    A test that iterates `app.routes` on a FastAPI version that nests included
    routers sees only `/docs` and friends, and every assertion below passes because
    it is asserting nothing. That is a green test for an empty set, which is the
    failure mode this module exists to avoid -- so it is checked first.

    The paths here are the ones on the nested route objects, so they carry no
    `include_router` prefix: the login route reads `/login`, not `/auth/login`. Only
    the OpenAPI schema has the prefixed form, and that is what the client sees.
    """
    from backend.main import create_app

    paths = {label.split(" ", 1)[1] for label, _endpoint in _routes(create_app())}

    assert "/login" in paths, f"the walk missed the login route; saw {sorted(paths)}"
    assert "/orders" in paths, f"the walk missed /orders; saw {sorted(paths)}"
    assert len(paths) > 5, f"only found {len(paths)} routes: {sorted(paths)}"


def test_every_route_handler_has_resolvable_annotations() -> None:
    """`get_type_hints` must succeed for every handler, in every module.

    This is the exact failure the login bug turned on: FastAPI calls
    `get_type_hints`-equivalent resolution on the handler and, on failure, falls back
    to treating the parameter as a query parameter. Raising here is what FastAPI
    declined to do.
    """
    from backend.main import create_app

    unresolved: list[str] = []
    for label, endpoint in _routes(create_app()):
        module = inspect.getmodule(endpoint)
        if module is None:
            unresolved.append(f"{label}: defined in a module that cannot be found")
            continue
        try:
            typing.get_type_hints(endpoint, vars(module))
        except NameError as exc:
            unresolved.append(
                f"{label} -> {endpoint.__qualname__}: {exc}. Names in a PEP 563 "
                "annotation must be module globals of the defining module."
            )
        except Exception as exc:  # noqa: BLE001
            unresolved.append(f"{label} -> {endpoint.__qualname__}: {type(exc).__name__}: {exc}")

    assert not unresolved, "unresolvable route annotations:\n  " + "\n  ".join(unresolved)


def test_route_handlers_are_defined_at_module_scope() -> None:
    """A handler must be a module-level function, not a closure.

    Redundant with the resolution test above, deliberately. Resolution is the
    property that matters, but it can pass by luck -- an inner handler whose
    annotations happen to name only module globals resolves fine, and the `Depends`
    target can still be a local that a future edit will rename. Asserting the shape
    gives a clearer failure message than a `NameError` discovered at request time.
    """
    from backend.main import create_app

    nested: list[str] = []
    for label, endpoint in _routes(create_app()):
        qualname = getattr(endpoint, "__qualname__", "")
        if "<locals>" in qualname:
            nested.append(f"{label} -> {qualname}")

    assert not nested, (
        "route handlers defined inside a function are invisible to FastAPI's "
        "annotation resolution under PEP 563:\n  " + "\n  ".join(nested)
    )
