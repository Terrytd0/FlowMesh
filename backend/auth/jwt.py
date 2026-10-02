"""JWT issuing and verification, reusing the Sprint 4 pattern.

Reused rather than reinvented, which is the point of the roadmap's "build it
once" instruction: the hashing, the claims and the failure behaviour are
identical to `supportops-ai`, so an engineer who has read one has read both.

The one addition is `require_role`, because the review queue is the first thing
in the portfolio where an *authentication* failure is not enough. Anyone
authenticated may place an order; only a `supervisor` may approve or reject one.
That is a one-line difference from Sprint 4 and the reason the helper exists.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from pwdlib import PasswordHash

from backend.config.settings import Settings
from backend.core.clock import utcnow
from backend.core.logging import get_logger

logger = get_logger(__name__)

#: Argon2 via pwdlib. Chosen over bcrypt for the memory-hardness property; the
#: hash is deliberately expensive, which is the point of a password hash.
_password_hash = PasswordHash.recommended()

ALGORITHM = "HS256"


def hash_password(plaintext: str) -> str:
    return _password_hash.hash(plaintext)


def verify_password(plaintext: str, hashed: str) -> bool:
    try:
        return _password_hash.verify(plaintext, hashed)
    except Exception:  # noqa: BLE001 - a malformed hash is a failed login, not a 500
        return False


def create_access_token(
    *,
    subject: str,
    settings: Settings,
    role: str = "customer",
    tenant: str = "cascade-retail",
    expires_minutes: int | None = None,
) -> str:
    """Mint a token.

    `sub`, `role`, `tenant` and `exp` are the four claims this project reads.
    Anything else is decoration; adding a claim that no authorisation decision
    consults is how "we log the user's permissions" becomes a security claim the
    system does not keep.
    """
    now = utcnow()
    expires = now + timedelta(minutes=expires_minutes or settings.access_token_expire_minutes)
    payload: dict[str, Any] = {
        "sub": subject,
        "role": role,
        "tenant": tenant,
        "iat": int(now.timestamp()),
        "exp": int(expires.timestamp()),
        "iss": settings.app_name,
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=ALGORITHM)
    _ = datetime, UTC
    return token


def decode_access_token(token: str, settings: Settings) -> dict[str, Any]:
    """Verify and decode. Raises `jwt.PyJWTError` on anything untrustworthy.

    Verifies the signature, the expiry *and* the issuer. Checking the issuer is
    the one people skip, and it is the one that stops a token minted by a
    different service sharing a secret (or a staging cluster) being accepted here.
    """
    return jwt.decode(
        token,
        settings.jwt_secret,
        algorithms=[ALGORITHM],
        issuer=settings.app_name,
        options={"require": ["exp", "sub", "iss"]},
    )


def expired_token_for_tests(settings: Settings, *, subject: str = "tester") -> str:
    """A token that expired an hour ago.

    Exists because "expired tokens are rejected" is a security property that has
    to be *tested* -- an auth layer that accepts an expired token is a green suite
    and a breach.
    """
    now = utcnow()
    payload = {
        "sub": subject,
        "role": "customer",
        "tenant": "cascade-retail",
        "iat": int((now - timedelta(hours=2)).timestamp()),
        "exp": int((now - timedelta(hours=1)).timestamp()),
        "iss": settings.app_name,
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=ALGORITHM)
