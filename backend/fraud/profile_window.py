"""The rolling per-customer window: streaming feature state.

This is the "online inference" half of the sprint. A fraud model that only sees
one order at a time can see a large basket; it cannot see that the same customer
placed four identical orders in nine minutes. Those features -- velocity, order
value relative to history, BIN switching, chargeback rate -- only exist as a
function of *recent events*, which is why they are computed here, at scoring
time, from a window rather than read off a batch table.

The window is a bounded LRU of customer profiles:

- **Bounded**, because this process holds one entry per customer seen in the
  window and an unbounded map is how a long-running scorer becomes the incident.
  200k profiles at roughly 300 bytes each is ~60MB, which is a number worth
  knowing rather than discovering.
- **LRU, not TTL-only**, because a flash sale produces a long tail of one-time
  customers who must be evictable, and recency is the better eviction signal
  than age for exactly that shape of traffic.
- **In memory, single process**, which is the honest limitation: with three
  scoring replicas, a customer's velocity is a third of what one replica sees.
  The fix is a shared window store (Redis, or Kafka Streams state), and it is
  written down in docs/architecture.md rather than quietly pretended away.

`record()` is called *after* the decision, so a customer's own current order
never counts toward their own velocity. Getting that order wrong -- recording
before scoring -- inflates every score by one order and makes the velocity
feature look like noise.
"""

from __future__ import annotations

import math
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from backend.core.clock import utcnow

#: Cap on tracked customers. Documented in the module docstring.
DEFAULT_CAPACITY = 200_000

#: Below this many prior orders a z-score is not meaningful -- one order has no
#: spread, so `stddev == 0` and the ratio is infinite. The feature falls back to
#: "new customer", which is a weaker but honest signal.
_MIN_ORDERS_FOR_ZSCORE = 3

#: Clamp on the z-score. Beyond this the linear model extrapolates to certainty
#: on the strength of one outlier, and one 10-sigma customer poisons every
#: subsequent feature.
_ZSCORE_CLAMP = 4.0

#: Orders-per-15-minutes that saturates the velocity feature.
_VELOCITY_SATURATION = 5.0


@dataclass
class CustomerProfile:
    """One customer's rolling state.

    `bins` is a small set rather than a counter: "has this customer used two
    different BINs" is the signal, not "how many times", and a set keeps it to a
    single feature.
    """

    customer_id: str
    order_count: int = 0
    total_cents: int = 0
    total_cents_sq: float = 0.0
    chargebacks: int = 0
    bins: set[str] = field(default_factory=set)
    recent: list[tuple[datetime, int]] = field(default_factory=list)
    touched_at: datetime = field(default_factory=utcnow)

    def mean_cents(self) -> float:
        return self.total_cents / self.order_count if self.order_count else 0.0

    def stddev_cents(self) -> float:
        if self.order_count < 2:
            return 0.0
        variance = self.total_cents_sq / self.order_count - self.mean_cents() ** 2
        # Clamp at zero: floating-point subtraction of two large similar numbers
        # can go slightly negative, and `sqrt(-1e-9)` is a NaN that silently
        # poisons every later score for this customer.
        return math.sqrt(max(0.0, variance))

    def chargeback_rate(self) -> float:
        return self.chargebacks / self.order_count if self.order_count else 0.0


class CustomerWindow:
    """A bounded, recency-ordered map of `CustomerProfile`."""

    def __init__(self, *, capacity: int = DEFAULT_CAPACITY, window_seconds: int = 900) -> None:
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        self._capacity = capacity
        self._window = timedelta(seconds=window_seconds)
        self._profiles: OrderedDict[str, CustomerProfile] = OrderedDict()
        self.evictions = 0
        # `default=None`, so a feature read outside any pinned context sees no
        # subject rather than raising -- `velocity()` outside a scoring call
        # returning 0 is the documented behaviour, and a ContextVar with no default
        # would turn that into a LookupError at every call site that checks it.
        self._current: ContextVar[CustomerProfile | None] = ContextVar(
            "flowmesh_scoring_subject", default=None
        )

    def __len__(self) -> int:
        return len(self._profiles)

    @property
    def capacity(self) -> int:
        return self._capacity

    def get(self, customer_id: str) -> CustomerProfile | None:
        """Read a profile and mark it most-recently-used.

        Returns `None` for an unknown customer rather than creating one: a read
        must not have the side effect of allocating, or a scoring pass over a
        hostile stream becomes a memory-exhaustion vector with extra steps.
        """
        profile = self._profiles.get(customer_id)
        if profile is not None:
            self._profiles.move_to_end(customer_id)
        return profile

    def record(
        self,
        *,
        customer_id: str,
        total_cents: int,
        card_bin: str,
        at: datetime | None = None,
        chargeback: bool = False,
    ) -> CustomerProfile:
        """Fold one order into the window. Call *after* the decision."""
        moment = at or utcnow()
        profile = self._profiles.get(customer_id)
        if profile is None:
            profile = CustomerProfile(customer_id=customer_id)
            self._profiles[customer_id] = profile
            while len(self._profiles) > self._capacity:
                self._profiles.popitem(last=False)
                self.evictions += 1

        profile.order_count += 1
        profile.total_cents += total_cents
        profile.total_cents_sq += float(total_cents) ** 2
        if chargeback:
            profile.chargebacks += 1
        if card_bin:
            profile.bins.add(card_bin)
        profile.recent.append((moment, total_cents))
        cutoff = moment - self._window
        profile.recent = [entry for entry in profile.recent if entry[0] >= cutoff]
        profile.touched_at = moment
        self._profiles.move_to_end(customer_id)
        return profile

    def mark_chargeback(self, customer_id: str) -> None:
        """Record a chargeback against a customer, for the next order's features.

        Exists as its own call because chargebacks arrive out of band -- from a
        payment processor webhook days later, not from the order stream -- and
        folding that into `record()` would mean scoring an order to discover a
        chargeback that already happened.
        """
        profile = self._profiles.get(customer_id)
        if profile is not None:
            profile.chargebacks += 1

    # -- derived features --------------------------------------------------
    #
    # These read `current()` rather than taking a `customer_id`. Threading the id
    # through six methods that all receive it from the same call site is how one of
    # them ends up reading a different customer's profile -- a bug that produces
    # plausible numbers and no error. `scoring_context` pins the subject,
    # `release_context` always clears it in a `finally`.
    #
    # The pin lives in a `ContextVar`, not in an attribute, and that is the second
    # half of the same argument. It started as `self._current`, which is a single
    # slot on a single object shared by every concurrent scoring call in the
    # process -- and `score()` awaits between pinning and releasing (the reasoner,
    # for one). So two interleaved scores for two customers would read each
    # other's profiles: a cross-customer data leak producing plausible numbers and
    # no error, which is precisely the failure mode the pinning was introduced to
    # prevent. It was not caught by the suite because every test scored one order
    # at a time.
    #
    # A `ContextVar` is task-local: each asyncio task gets a copy of the context at
    # creation, so a `set()` inside one scoring call is invisible to another, and
    # the reset token restores the previous value even if the pin was never
    # released. `_context_is_task_local` asserts the interleaving directly.

    def current(self) -> CustomerProfile | None:
        """The profile for the customer currently being scored, if known."""
        return self._current.get()

    def scoring_context(self, customer_id: str) -> Token[CustomerProfile | None]:
        """Pin `customer_id` as the subject for the derived features.

        Returns the token needed to unpin. Prefer the `scoring_context(...)`
        context manager below, which cannot leak the pin by forgetting to reset.
        """
        profile = self._profiles.get(customer_id)
        if profile is not None:
            self._profiles.move_to_end(customer_id)
        return self._current.set(profile)

    def release_context(self, token: Token[CustomerProfile | None] | None = None) -> None:
        """Unpin the subject.

        Takes the token from `scoring_context` so nesting restores rather than
        clobbers. Without one it clears the pin outright, which is the right
        behaviour for a handler unwinding after an exception.
        """
        if token is None:
            self._current.set(None)
        else:
            self._current.reset(token)

    @contextmanager
    def pinned(self, customer_id: str) -> Iterator[CustomerProfile | None]:
        """Pin for the duration of a `with` block.

        `finally`-based by construction, so the one mistake that matters -- an early
        return or a raised exception leaving the pin set -- is not expressible.
        """
        token = self.scoring_context(customer_id)
        try:
            yield self._current.get()
        finally:
            self._current.reset(token)

    def amount_zscore(self, total_cents: int) -> float:
        """How far this order sits above the customer's own history, clamped.

        Only *positive* deviations count. A customer's order being cheaper than
        usual is not fraud evidence, and a symmetric feature would push ordinary
        small repeat purchases toward review.
        """
        profile = self.current()
        if profile is None or profile.order_count < _MIN_ORDERS_FOR_ZSCORE:
            return 0.0
        stddev = profile.stddev_cents()
        if stddev <= 0.0:
            # No spread in the history (e.g. three identical orders). Treat any
            # increase as large but bounded -- claiming `inf` would let a
            # customer with a flat history reach a score of 1.0 forever.
            if total_cents > profile.mean_cents():
                return float(_ZSCORE_CLAMP)
            return 0.0
        z = (total_cents - profile.mean_cents()) / stddev
        return max(0.0, min(_ZSCORE_CLAMP, z))

    def velocity(self, at: datetime | None = None) -> int:
        """Orders in the window for the **pinned** customer.

        Reads the scoring context, so it is only meaningful inside a
        `scoring_context(...)` block and returns 0 outside one. That is fine for
        the feature path (which is always inside the block) and a trap for
        everything else -- `tests/unit/fraud/test_engine.py` found it by asking
        "how many orders has this customer made?" after scoring had finished and
        being told zero.

        Use `velocity_for(customer_id)` to ask about a specific customer.
        """
        profile = self.current()
        if profile is None:
            return 0
        return _velocity(profile, at, self._window)

    def velocity_for(self, customer_id: str, at: datetime | None = None) -> int:
        """Orders in the window for `customer_id`, explicitly.

        Does not disturb the pinned scoring context, so this is safe to call from
        a dashboard, a test, or the chaos report while scoring is in flight.
        """
        profile = self._profiles.get(customer_id)
        if profile is None:
            return 0
        return _velocity(profile, at, self._window)

    def velocity_feature(self, at: datetime | None = None) -> float:
        """Velocity scaled to [0, 1], saturating at `_VELOCITY_SATURATION`."""
        return min(1.0, self.velocity(at) / _VELOCITY_SATURATION)

    def velocity_feature_for(self, customer_id: str, at: datetime | None = None) -> float:
        """The scaled velocity for a named customer, without pinning anything."""
        return min(1.0, self.velocity_for(customer_id, at) / _VELOCITY_SATURATION)

    def known_bins(self) -> set[str]:
        profile = self.current()
        return set(profile.bins) if profile is not None else set()

    def order_count(self) -> int:
        profile = self.current()
        return profile.order_count if profile is not None else 0

    def chargeback_rate(self) -> float:
        profile = self.current()
        return profile.chargeback_rate() if profile is not None else 0.0

    def order_count_for(self, customer_id: str) -> int:
        """Prior orders for a named customer, without pinning anything."""
        profile = self._profiles.get(customer_id)
        return profile.order_count if profile is not None else 0


def _velocity(profile: CustomerProfile, at: datetime | None, window: timedelta) -> int:
    """Orders recorded for `profile` inside the window ending at `at`."""
    cutoff = (at or utcnow()) - window
    return sum(1 for seen_at, _ in profile.recent if seen_at >= cutoff)
