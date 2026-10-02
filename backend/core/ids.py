"""Identity helpers.

Three id families, and the distinction matters more than it looks:

- **Event ids** identify *one fact that happened* (`order.accepted`). They are
  the idempotency key: a handler that has already recorded an event id must not
  apply the effect twice. Kafka's at-least-once delivery guarantee makes this
  the normal case, not the exception.
- **Correlation ids** identify *one order's journey* across the API, the event
  log, the gRPC call, the review queue and the audit log. One customer
  complaint is reconstructable from a single grep.
- **Idempotency keys** are supplied by the *caller* of the order API, not
  generated here. A mobile client that retries a POST after a dropped response
  must not create two orders, and only the caller can know the two requests are
  the same one.
"""

from __future__ import annotations

import uuid


def new_event_id() -> str:
    """A unique id for one occurrence of one fact."""
    return uuid.uuid4().hex


def new_correlation_id() -> str:
    """Ties one order's events together across every process that touches it."""
    return uuid.uuid4().hex


def new_order_id() -> str:
    """Customer-facing order reference, e.g. `CRG-2026-4f9a2c1b`.

    Prefixed and dated rather than a bare UUID because order numbers get read
    aloud to customers, pasted into support tickets and quoted to warehouse
    staff; `CRG-2026-4f9a2c1b` survives that round trip and a UUID does not.
    """
    from backend.core.clock import utcnow

    year = utcnow().year
    return f"CRG-{year}-{uuid.uuid4().hex[:8]}"


def new_review_id() -> str:
    """Review-queue key, e.g. `REV-4f9a2c1b`."""
    return f"REV-{uuid.uuid4().hex[:8]}"


def deterministic_cache_key(*parts: object) -> str:
    """A stable cache key from the feature vector.

    Used to cache LLM rationales. The features must hash identically across
    processes, so this takes only scalars and uses `repr` -- a dict's iteration
    order is stable in Python but relying on a JSON blob's key order for a
    cache key is the sort of thing that silently halves a cache hit rate after
    an unrelated refactor.
    """
    import hashlib

    raw = "|".join(repr(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
