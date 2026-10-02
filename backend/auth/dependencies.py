"""Auth dependencies and the login route.

The shape is Sprint 4's, with one addition: `require_role("supervisor")`. The
review queue is the first surface in this portfolio where being *authenticated*
is not sufficient, and it is the only place a role is checked.

Two things this module deliberately does not do:

- **No refresh tokens.** A 60-minute access token with no refresh means a
  session either works or is re-issued; there is no third state where a stolen
  refresh token outlives its theft. For a portfolio project that is the right
  default, and the gap is named here rather than left as a TODO someone will
  find.
- **No user table.** Roles come from the token's claims, and the only user store
  is the seed file. A real deployment would read roles from a directory; a
  portfolio project that invents a user database teaches the wrong lesson about
  where identity belongs.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from backend.auth.jwt import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)
from backend.config.settings import Settings, get_settings
from backend.core.logging import get_logger
from backend.database.session import get_session

logger = get_logger(__name__)

_bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    """The authenticated caller."""

    subject: str
    role: str
    tenant: str

    @property
    def is_supervisor(self) -> bool:
        return self.role in ("supervisor", "admin")


def get_principals_from_settings(settings: Settings) -> dict[str, dict[str, str]]:
    """The development user store.

    Read from `SEED_USERS`-style config rather than a database: this is a demo
    credential store, and putting it in a table would suggest it is the real
    identity system.
    """
    return {
        "customer@example.com": {
            "password": "customer-pass",
            "role": "customer",
            "subject": "CUST-1",
        },
        "supervisor@example.com": {
            "password": "supervisor-pass",
            "role": "supervisor",
            "subject": "SUP-1",
        },
        "admin@example.com": {"password": "admin-pass", "role": "admin", "subject": "ADM-1"},
    }


async def get_principal(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> Principal:
    """FastAPI dependency: authenticate the caller.

    With `auth_enabled=False` (local dev only) returns an anonymous supervisor so
    `docker compose up` is explorable without minting tokens first. Startup
    refuses that combination in production -- see `backend/main.py`.
    """
    if not settings.auth_enabled:
        return Principal(subject="anonymous", role="admin", tenant="cascade-retail")
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        claims = decode_access_token(credentials.credentials, settings)
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="token expired",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except jwt.PyJWTError as exc:
        logger.warning("rejected token: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    return Principal(
        subject=str(claims["sub"]),
        role=str(claims.get("role", "customer")),
        tenant=str(claims.get("tenant", "cascade-retail")),
    )


def require_role(role: str) -> Any:
    """Build a dependency that also checks the role.

    Returns a dependency rather than checking inside a route, so the requirement
    is declared next to the other dependencies on the route and cannot be
    forgotten when a handler is refactored.
    """

    async def _dependency(
        principal: Annotated[Principal, Depends(get_principal)],
    ) -> Principal:
        if principal.role not in (role, "admin"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"role {principal.role!r} cannot perform this action; {role!r} required",
            )
        return principal

    return _dependency


CurrentPrincipal = Annotated[Principal, Depends(get_principal)]
SupervisorPrincipal = Annotated[Principal, Depends(require_role("supervisor"))]
DbSession = Annotated[AsyncSession, Depends(get_session)]


class AuthService:
    """Login, without a user table."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._users = get_principals_from_settings(settings)
        # Hashed at construction rather than per login. Verifying against a plain
        # stored password would be a timing-attack surface and a bad habit worth
        # not having in a portfolio.
        self._hashed: dict[str, str] = {
            email: hash_password(record["password"]) for email, record in self._users.items()
        }

    def login(self, email: str, password: str) -> dict[str, object] | None:
        """`None` on bad credentials; a token payload on success."""
        record = self._users.get(email.lower())
        if record is None:
            # Hash anyway, so a missing user and a wrong password take the same
            # time. Otherwise response latency enumerates valid accounts.
            verify_password(password, "$argon2$fake$hash$for$timing$parity")
            return None
        if not verify_password(password, self._hashed[email.lower()]):
            return None
        token = create_access_token(
            subject=record["subject"], settings=self._settings, role=record["role"]
        )
        return {
            "access_token": token,
            "token_type": "bearer",
            "role": record["role"],
            "subject": record["subject"],
        }


async def session_dependency() -> AsyncGenerator[AsyncSession]:
    """Exposed for tests that want the same session the routes use."""
    async for session in get_session():
        yield session
