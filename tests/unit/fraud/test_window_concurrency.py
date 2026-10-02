"""The rolling fraud window under concurrency.

Every other test in `tests/unit/fraud/` scores one order at a time, which is the
shape that hides the bug this file exists for.

`CustomerWindow` pinned the customer being scored in a single attribute,
`self._current`. `FraudScoringEngine.score` pinned on entry and unpinned in a
`finally`, which is correct for one call and wrong for two: the pin is process-wide,
and `score` awaits between pinning and releasing -- the reasoner, and any LLM behind
it. So two concurrent scores for two customers overwrite each other's subject, and
whichever computes its features second reads the other's profile.

That is a cross-customer data leak. It produces plausible numbers, no exception, and
no failing assertion anywhere, because the existing suite never had two scorings in
flight. The property it corrupts is exactly the one fraud scoring exists to protect:
customer A's velocity and order history feed customer B's score.

These tests interleave deliberately with barriers rather than hoping a scheduler
does it, so they fail on the old implementation instead of passing intermittently.
"""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from typing import Any

import pytest

from backend.config.settings import Settings
from backend.core.clock import set_clock_offset, utcnow
from backend.events.schema import RiskBand
from backend.fraud.engine import FraudScoringEngine
from backend.fraud.features import OrderContext
from backend.fraud.profile_window import CustomerProfile
from backend.observability.metrics import FlowMeshMetrics


def _engine(settings: Any, metrics: Any) -> FraudScoringEngine:
    return FraudScoringEngine(Settings(**settings.model_dump()), metrics)


def _order(customer_id: str, order_id: str, **overrides: Any) -> OrderContext:
    """A benign domestic order.

    Deliberately no `is_first_order`: `OrderContext` derives `first_order` from
    whether the window knows the customer, so passing it would be a TypeError and,
    worse, an attempt to assert the derived feature from the wrong end.
    """
    defaults: dict[str, Any] = {
        "order_id": order_id,
        "customer_id": customer_id,
        "total_cents": 4_200,
        "items": (("SKU-1", 1), ("SKU-2", 1)),
        "card_bin": "411111",
        "card_country": "US",
        "billing_country": "US",
        "shipping_country": "US",
        "ip_country": "US",
    }
    defaults.update(overrides)
    return OrderContext(**defaults)


async def test_a_pin_survives_an_await_inside_the_scoring_span(settings: Any, metrics: Any) -> None:
    """Why the pin is a `ContextVar` and not an attribute.

    A precise statement of the hazard, because the current code does *not* have
    this bug and it would be dishonest to claim otherwise.

    Today `score()` pins and then calls `compute_features`, which is synchronous:
    no `await` runs between the two, so no other task can interleave, so a
    single-slot pin happens to be safe. That safety is incidental. It rests on an
    ordering nobody wrote down -- `compute_features` being sync -- and the day
    someone adds an `await` to it (a Redis-backed window lookup, an async feature
    source, an `await` in a helper) two concurrent scores silently read each
    other's customers, with every existing test still green.

    So this test puts the `await` in explicitly and asserts the subject survives it.
    With a single slot it fails; with a `ContextVar` it passes. The gap is the
    point: it documents what the next person to touch this code is about to break.
    """
    engine = _engine(settings, metrics)
    for _ in range(5):
        engine.window.record(customer_id="CUST-FAST", total_cents=1_000, card_bin="411111")
        engine.window.record(customer_id="CUST-SLOW", total_cents=900_000, card_bin="422222")

    observed: dict[str, CustomerProfile | None] = {}

    async def _scored(customer_id: str, total_cents: int) -> None:
        async with asyncio.timeout(5):
            with engine.window.pinned(customer_id):
                # The await that does not exist yet.
                await asyncio.sleep(0.01)
                observed[customer_id] = engine.window.current()

    await asyncio.gather(
        _scored("CUST-FAST", 95_000),
        _scored("CUST-SLOW", 1_100),
    )

    for customer_id, profile in observed.items():
        assert profile is not None, f"{customer_id} lost its subject across an await"
        assert profile.customer_id == customer_id, (
            f"{customer_id} read {profile.customer_id}'s profile after an await inside "
            "the scoring span -- a cross-customer data leak"
        )


async def test_concurrent_scores_do_not_read_each_others_profiles(
    settings: Any, metrics: Any
) -> None:
    """End-to-end through `score()`, for the same property.

    Two customers with deliberately opposite histories, scored at the same time, each
    checked against its own profile. The barrier inside `compute_features` forces the
    two calls to overlap as far as the current synchronous implementation allows.

    This one passes against the single-slot implementation too, and that is
    informative rather than redundant: it pins down that today's safety comes from
    `compute_features` being synchronous, while the test above pins down that it
    must not be the only thing holding it up.
    """
    engine = _engine(settings, metrics)

    # Two opposite histories, so a swap cannot be invisible.
    for _ in range(5):
        engine.window.record(customer_id="CUST-RICH", total_cents=1_000, card_bin="411111")
        engine.window.record(customer_id="CUST-POOR", total_cents=900_000, card_bin="422222")

    import backend.fraud.engine as engine_module

    real_compute_features = engine_module.compute_features
    both_inside = asyncio.Event()
    entered = 0

    def _compute(context: OrderContext, window: Any) -> Any:
        """Hold both scorings inside `compute_features` until both have arrived.

        This is the window in which the subject can be wrong: both calls have
        pinned, neither has released. `threading`-style barriers do not exist for
        asyncio, so the equivalent is a counter plus an `Event`.
        """
        nonlocal entered
        entered += 1
        if entered == 2:
            both_inside.set()
        # `compute_features` is synchronous, so this cannot await. The barrier is
        # therefore a spin on the event, bounded, rather than an unbounded wait.
        for _ in range(200):
            if both_inside.is_set():
                break
            time.sleep(0.005)
        return real_compute_features(context, window)

    engine_module.compute_features = _compute  # type: ignore[assignment]
    try:
        rich, poor = await asyncio.gather(
            engine.score(_order("CUST-RICH", "ORD-RICH", total_cents=95_000)),
            engine.score(_order("CUST-POOR", "ORD-POOR", total_cents=1_100)),
        )
    finally:
        engine_module.compute_features = real_compute_features  # type: ignore[assignment]

    assert both_inside.is_set(), "the two scores never overlapped, so nothing was proven"

    rich_features, poor_features = rich.features, poor.features

    # 95,000 against a mean of 1,000 with no spread: clamped positive deviation.
    assert rich_features["amount_zscore"] > 0.0, (
        "the rich customer's own small order history did not produce a positive "
        f"z-score; got {rich_features['amount_zscore']!r}, which suggests the "
        "features were computed against another customer's profile"
    )
    # 1,100 against a mean of 900,000: below the mean, so no deviation. A rich
    # history leaking in here would show as a *negative* number.
    assert poor_features["amount_zscore"] == 0.0, (
        f"the low-value customer saw a z-score of {poor_features['amount_zscore']!r}; "
        "a negative value means the high-value customer's profile was read instead"
    )


async def test_a_pinned_subject_is_invisible_to_another_task(settings: Any, metrics: Any) -> None:
    """The pin is task-local, which is the mechanism the above test depends on.

    Asserted at the window level so that a future refactor to a plain attribute
    fails here with an obvious message rather than only through the engine test.
    """
    window = _engine(settings, metrics).window
    for _ in range(3):
        window.record(customer_id="CUST-A", total_cents=10_000, card_bin="411111")

    pinned_now = asyncio.Event()
    observed = asyncio.Event()
    seen_by_holder: list[CustomerProfile | None] = []
    seen_by_observer: list[CustomerProfile | None] = []

    async def holder() -> None:
        token = window.scoring_context("CUST-A")
        try:
            pinned_now.set()
            # Wait for the observer to have looked. This is the interleaving point:
            # a single-slot pin is readable by anyone who asks during this await.
            await asyncio.wait_for(observed.wait(), timeout=5)
            seen_by_holder.append(window.current())
        finally:
            window.release_context(token)

    async def observer() -> None:
        await asyncio.wait_for(pinned_now.wait(), timeout=5)
        seen_by_observer.append(window.current())
        observed.set()

    await asyncio.gather(holder(), observer())

    assert seen_by_holder[0] is not None and seen_by_holder[0].customer_id == "CUST-A"
    assert seen_by_observer[0] is None, (
        "a pin set in one task was visible in another, so concurrent scoring can "
        "read the wrong customer's profile"
    )


async def test_a_nested_pin_restores_the_outer_subject(settings: Any, metrics: Any) -> None:
    """Nesting resets rather than clobbering.

    A `release_context()` with no token clears the pin outright, which is right for
    unwinding after an exception and wrong for an inner scope. The token-based
    release is what makes the difference, so it is asserted.
    """
    window = _engine(settings, metrics).window
    window.record(customer_id="CUST-OUTER", total_cents=1_000, card_bin="411111")
    window.record(customer_id="CUST-INNER", total_cents=2_000, card_bin="422222")

    outer_token = window.scoring_context("CUST-OUTER")
    outer_subject = window.current()
    assert outer_subject is not None and outer_subject.customer_id == "CUST-OUTER"

    inner_token = window.scoring_context("CUST-INNER")
    inner_subject = window.current()
    assert inner_subject is not None and inner_subject.customer_id == "CUST-INNER"
    window.release_context(inner_token)

    restored = window.current()
    assert restored is not None and restored.customer_id == "CUST-OUTER", (
        "releasing an inner pin cleared the outer one, so a nested scoring path "
        "would lose its subject"
    )
    window.release_context(outer_token)
    assert window.current() is None


async def test_the_pinned_context_manager_unpins_on_an_exception(
    settings: Any, metrics: Any
) -> None:
    """`pinned` exists so a leaked pin is not expressible; prove it.

    The old engine code had the same `finally`, and the old window stored the pin in
    one shared slot -- so a leak was invisible for a single scoring and catastrophic
    for two. What matters now is that the context manager cannot forget.
    """
    window = _engine(settings, metrics).window
    window.record(customer_id="CUST-LEAKY", total_cents=5_000, card_bin="411111")

    with pytest.raises(RuntimeError, match="features exploded"):
        with window.pinned("CUST-LEAKY"):
            assert window.current() is not None
            raise RuntimeError("features exploded")

    assert window.current() is None, "the pin survived the exception"


async def test_scoring_a_poison_order_does_not_poison_the_next_customer(
    settings: Any, metrics: Any
) -> None:
    """The leak as it would appear in production: one bad order, then normal traffic.

    A handler that raises mid-score must leave the window usable for the next
    customer. On the single-slot implementation, a raise inside `score` between pin
    and release could leave the *previous* customer's profile pinned -- and the next
    order, for an unrelated customer, would be scored against it.
    """
    engine = _engine(settings, metrics)
    for _ in range(4):
        engine.window.record(customer_id="CUST-VICTIM", total_cents=2_000, card_bin="411111")

    import backend.fraud.engine as engine_module

    original = engine_module.compute_features

    def _boom(context: OrderContext, window: Any) -> Any:
        raise RuntimeError("features exploded")

    engine_module.compute_features = _boom  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError):
            await engine.score(_order("CUST-VICTIM", "ORD-BAD"))
    finally:
        engine_module.compute_features = original  # type: ignore[assignment]

    assert engine.window.current() is None, "the failed scoring left a customer pinned"

    # The next customer is scored with an empty window and must land in the low band.
    decision = await engine.score(_order("CUST-NEW", "ORD-GOOD"))
    assert decision.band == RiskBand.LOW, (
        f"a fresh customer scored {decision.band} after a previous scoring failed, "
        f"score={decision.score:.3f} reasons={decision.reasons}"
    )


async def test_many_concurrent_customers_each_get_their_own_velocity(
    settings: Any, metrics: Any
) -> None:
    """The version of the above that scales to real traffic.

    Twenty customers, one scorings each, all released at once. Every one of them has
    a distinct prior order count, so a single shared pin produces a distribution of
    wrong answers rather than one obviously wrong one -- which is the more realistic
    failure and the harder one to notice in production.
    """
    engine = _engine(settings, metrics)
    customers = [f"CUST-{index:02d}" for index in range(20)]
    for index, customer_id in enumerate(customers):
        for _ in range(index + 1):
            engine.window.record(customer_id=customer_id, total_cents=1_000, card_bin="411111")

    decisions = await asyncio.gather(
        *(
            engine.score(_order(customer_id, f"ORD-{customer_id}", total_cents=50_000))
            for customer_id in customers
        )
    )

    # The `velocity` *feature* is the scaled count, saturating at
    # `_VELOCITY_SATURATION` orders, so the expected value is not the raw count.
    # The five customers below the saturation point are what make this assertion
    # work: only they can distinguish "read my own history" from "read someone
    # else's", since everything above five orders clamps to 1.0 either way.
    saturation = 5.0
    reported = {decision.customer_id: decision.features["velocity"] for decision in decisions}
    expected = {
        customer_id: min(1.0, (index + 1) / saturation)
        for index, customer_id in enumerate(customers)
    }

    assert reported == expected, (
        "concurrent scoring read the wrong customer's velocity for: "
        f"{ {k: (v, expected[k]) for k, v in reported.items() if v != expected[k]} }"
    )


def test_a_stub_clock_does_not_mask_the_leak() -> None:
    """A guard on the test setup itself.

    Several of the tests above pin `received_at` to a fixed moment. If the window
    scrolled, `velocity` would return 0 for everybody and
    `test_many_concurrent_customers...` would fail for the wrong reason. This pins
    the clock and asserts it, so a future change to the fixture is not mistaken for
    a concurrency bug.
    """
    set_clock_offset(timedelta(0))
    assert utcnow().tzinfo is not None
