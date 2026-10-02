"""Redis-backed rate limiting and the idempotency-key cache.

The order endpoint is the only public, unauthenticated-ish surface in this
project, and it is the one that a load test will point 500 orders/sec at. Two
protections, both here:

**Rate limiting**, per API key / per subject, as a fixed window in Redis. A fixed
window rather than a sliding one because it is one `INCR` and one expiry, and
because its failure mode -- a 2x burst at a window boundary -- is a known,
bounded, acceptable inaccuracy. The alternative (sliding window) needs sorted sets
and buys an inaccuracy nobody would notice. What matters is that the limit is
*shared across replicas*, which is exactly why it is in Redis and not in a
process-local counter: a per-process limit is multiplied by the replica count, so
scaling out silently scales the limit out with it.

**Idempotency-key cache**, so a retried POST can be answered without touching the
database. The database's unique constraint is still the authority -- this is a
cache in front of it, and a cache that became the authority would be a second
source of truth for whether an order exists.

Both degrade to *allow* when Redis is unreachable. That is the right failure
direction for a rate limiter (failing closed turns a Redis outage into a total
order-processing outage, which is strictly worse) and the wrong one for
idempotency, which is why the idempotency authority is the database constraint
and not this cache.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    reset_seconds: int
    #: True when the check could not be made and was allowed through.
    degraded: bool = False


class RateLimiter:
    """Fixed-window limiter over Redis, with a pass-through fallback."""

    def __init__(self, *, url: str, limit_per_minute: int, client: Any = None) -> None:
        self._url = url
        self._limit = max(1, limit_per_minute)
        self._client = client
        self.degraded_checks = 0

    async def _ensure_client(self) -> Any:
        if self._client is None:
            from redis.asyncio import Redis

            self._client = Redis.from_url(self._url, decode_responses=True)
        return self._client

    async def check(self, key: str, *, window_seconds: int = 60) -> RateLimitResult:
        """Count one request against `key`'s current window."""
        from backend.core.clock import utcnow

        window = int(utcnow().timestamp()) // window_seconds
        redis_key = f"flowmesh:rl:{key}:{window}"
        try:
            client = await self._ensure_client()
            count = int(await client.incr(redis_key))
            if count == 1:
                # First request in this window: set the expiry so the key does not
                # live forever. `expire` on every request would extend the window
                # indefinitely under load and turn a limiter into a suggestion.
                await client.expire(redis_key, window_seconds * 2)
        except Exception as exc:  # noqa: BLE001 - see the module docstring
            self.degraded_checks += 1
            logger.warning("rate limiter unavailable, allowing request: %s", exc)
            return RateLimitResult(
                allowed=True,
                limit=self._limit,
                remaining=self._limit,
                reset_seconds=window_seconds,
                degraded=True,
            )
        return RateLimitResult(
            allowed=count <= self._limit,
            limit=self._limit,
            remaining=max(0, self._limit - count),
            reset_seconds=window_seconds,
        )

    async def close(self) -> None:
        if self._client is not None and hasattr(self._client, "aclose"):
            await self._client.aclose()
            self._client = None


class IdempotencyCache:
    """A short-lived cache of idempotency-key to order-id.

    Exists so a retry is answered in microseconds instead of a `SELECT`. It is a
    cache: the unique constraint on `orders.idempotency_key` remains the
    authority, and `POST /orders` still handles the constraint violation.
    """

    def __init__(self, *, url: str, ttl_seconds: int = 3600, client: Any = None) -> None:
        self._url = url
        self._ttl = ttl_seconds
        self._client = client
        self.degraded_checks = 0

    async def _ensure_client(self) -> Any:
        if self._client is None:
            from redis.asyncio import Redis

            self._client = Redis.from_url(self._url, decode_responses=True)
        return self._client

    async def get(self, key: str) -> str | None:
        try:
            client = await self._ensure_client()
            return await client.get(f"flowmesh:idem:{key}")
        except Exception as exc:  # noqa: BLE001
            self.degraded_checks += 1
            logger.debug("idempotency cache unavailable on read: %s", exc)
            return None

    async def put(self, key: str, order_id: str) -> None:
        try:
            client = await self._ensure_client()
            await client.set(f"flowmesh:idem:{key}", order_id, ex=self._ttl)
        except Exception as exc:  # noqa: BLE001
            self.degraded_checks += 1
            logger.debug("idempotency cache unavailable on write: %s", exc)

    async def close(self) -> None:
        if self._client is not None and hasattr(self._client, "aclose"):
            await self._client.aclose()
            self._client = None


class InMemoryRateLimiter(RateLimiter):
    """A `RateLimiter` with no Redis, for tests and single-process runs.

    Same semantics, one process. Used by the load test (which is a single
    process) and by the unit tests. It is a *different implementation*, not a
    mock, so a test that asserts on it is testing the policy rather than the
    storage.
    """

    def __init__(self, *, limit_per_minute: int) -> None:
        super().__init__(url="memory://", limit_per_minute=limit_per_minute, client={})
        self._counts: dict[str, int] = {}

    async def check(self, key: str, *, window_seconds: int = 60) -> RateLimitResult:
        from backend.core.clock import utcnow

        window = int(utcnow().timestamp()) // window_seconds
        redis_key = f"{key}:{window}"
        count = int(self._counts.get(redis_key, 0)) + 1
        self._counts[redis_key] = count
        return RateLimitResult(
            allowed=count <= self._limit,
            limit=self._limit,
            remaining=max(0, self._limit - count),
            reset_seconds=window_seconds,
        )

    async def close(self) -> None:
        return None


class InMemoryIdempotencyCache(IdempotencyCache):
    """The Redis cache's semantics, held in a dict."""

    def __init__(self) -> None:
        super().__init__(url="memory://", ttl_seconds=3600, client={})
        self._values: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self._values.get(key)

    async def put(self, key: str, order_id: str) -> None:
        self._values[key] = order_id

    async def close(self) -> None:
        return None
