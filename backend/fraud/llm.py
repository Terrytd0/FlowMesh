"""Why an order was scored the way it was.

Two reasoners behind one protocol:

- **`DeterministicReasoner`** (the default) builds the explanation from the
  model's own contributions. It costs microseconds, needs no network and no key,
  and it cannot hallucinate a reason the model did not use -- which is the
  property that matters, because the explanation is shown to a human deciding
  whether to ship an order.
- **`OpenAiReasoner`** spends an actual LLM call, and is only ever invoked for a
  score in the *ambiguous* band, with a rationale requested. Its value is
  nuance the arithmetic cannot express ("this basket is unremarkable but the
  customer changed address and card in the same order"); its cost is 300ms of
  best case and a tail with no upper bound, which is why it is off by default
  and why the fast path sets `require_rationale=False`.

The band restriction is the design decision, and it is worth being blunt about:
**an LLM on the hot path cannot meet a 200ms p95.** So the LLM is not a faster
scorer, it is a slower explainer called only where the model is unsure. That is
also why the deterministic reasoner exists at all -- without it, turning the LLM
off would leave the ambiguous band with no explanation, which is a worse product
than either extreme.

Rationales are cached on the feature vector. Two ambiguous orders from the same
kind of customer produce the same arithmetic, so the same sentence, so the second
one is free.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol

from backend.core.clock import monotonic
from backend.core.ids import deterministic_cache_key
from backend.core.logging import get_logger
from backend.events.schema import RiskBand
from backend.fraud.bands import REASON_LABELS
from backend.fraud.features import FeatureVector
from backend.fraud.model import Contribution
from backend.observability.metrics import FlowMeshMetrics

logger = get_logger(__name__)

#: Cap on how many rationales are cached. Bounded because an LLM rationale cache
#: keyed on arbitrary customer input is a memory-growth vector; 10k entries at
#: ~300 bytes is a few MB and covers the ambiguous band of a busy minute.
_CACHE_CAPACITY = 10_000


@dataclass(frozen=True)
class Rationale:
    """An explanation plus where it came from.

    `source` is returned to the caller and logged. "The LLM said" and "the model
    said" are different claims, and a review screen that shows one while an
    auditor assumes the other is a small incident with a large paper trail.
    """

    text: str
    source: str
    llm_used: bool = False


class LlmReasoner(Protocol):
    """One method, because there is exactly one thing asked of a reasoner."""

    async def explain(
        self,
        *,
        order_id: str,
        score: float,
        band: RiskBand,
        contributions: list[Contribution],
        features: FeatureVector,
    ) -> Rationale: ...


class DeterministicReasoner:
    """Builds the explanation from the model's own arithmetic.

    Templates rather than prose, deliberately: a generated sentence that
    misdescribes the maths is worse than a list, and this one cannot.
    """

    def __init__(self, *, limit: int = 4) -> None:
        self._limit = limit

    async def explain(
        self,
        *,
        order_id: str,
        score: float,
        band: RiskBand,
        contributions: list[Contribution],
        features: FeatureVector,
    ) -> Rationale:
        drivers = [row for row in contributions if row.value > 0.0][: self._limit]
        if not drivers:
            text = (
                f"No risk signal fired: score {score:.2f}, no feature above its floor. "
                "No behavioural or geographic anomaly detected on this order."
            )
            return Rationale(text=text, source="model")

        described = ", ".join(f"{row.label} ({row.value:.2f})" for row in drivers)
        total = sum(row.contribution for row in contributions)
        text = (
            f"Scored {score:.2f} ({band}). Driven by {described}. "
            f"Combined model contribution {total:+.2f} logits against a base rate of "
            f"{features['first_order'] and 'new customer' or 'known customer'}."
        )
        return Rationale(text=text, source="model")


class CachingReasoner:
    """Wraps another reasoner with an LRU on the feature vector.

    Sits outside `DeterministicReasoner` because it caches *answers*, and the
    deterministic one's answers are already cheap -- the point of the cache is
    the LLM. It caches both because the cache key is the same shape and having
    two cache implementations would be silly.
    """

    def __init__(self, inner: LlmReasoner, *, capacity: int = _CACHE_CAPACITY) -> None:
        self._inner = inner
        self._capacity = capacity
        self._cache: dict[str, Rationale] = {}
        self.hits = 0
        self.misses = 0

    def _key(self, features: FeatureVector, band: RiskBand) -> str:
        # The band is part of the key: the same vector on either side of a
        # threshold gets a different sentence ("held" vs "shipped"), so a key
        # without it would serve the wrong one after a threshold change.
        return deterministic_cache_key(band.value, *features.vector())

    async def explain(
        self,
        *,
        order_id: str,
        score: float,
        band: RiskBand,
        contributions: list[Contribution],
        features: FeatureVector,
    ) -> Rationale:
        key = self._key(features, band)
        cached = self._cache.get(key)
        if cached is not None:
            self.hits += 1
            return cached
        self.misses += 1
        rationale = await self._inner.explain(
            order_id=order_id,
            score=score,
            band=band,
            contributions=contributions,
            features=features,
        )
        if len(self._cache) >= self._capacity:
            self._cache.pop(next(iter(self._cache)))
        self._cache[key] = rationale
        return rationale


class OpenAiReasoner:
    """The LLM-backed reasoner, for the ambiguous band only.

    Fails soft by design: a timeout, a rate limit or a malformed response yields
    the deterministic rationale instead of failing the order. The alternative --
    failing the scoring call because an explanation was unavailable -- would push
    every LLM outage into the order pipeline, for a sentence nobody is required
    to read.
    """

    #: Asked for a verdict we can ignore. The reasoner does **not** get to change
    #: the decision: the model owns the score, the LLM owns the prose. Letting an
    #: LLM move a score means the score stops being auditable, which is the one
    #: property a fraud decision cannot lose.
    _SYSTEM = (
        "You explain fraud-risk scores to a retail fraud analyst. "
        "You are given a score, a band, and the model's per-feature contributions. "
        "Write two sentences: what drove the score, and what a reviewer should check. "
        "Never contradict the contributions you are given, never claim a signal that "
        "is not listed, and never state a probability other than the one provided. "
        "No preamble, no bullet points."
    )

    def __init__(
        self,
        *,
        fallback: LlmReasoner,
        metrics: FlowMeshMetrics,
        model: str = "gpt-4o-mini",
        api_key: str = "",
        base_url: str = "",
        timeout_seconds: float = 3.0,
        max_tokens: int = 200,
        client: Any = None,
    ) -> None:
        self._fallback = fallback
        self._metrics = metrics
        self._model = model
        self._api_key = api_key
        self._base_url = base_url
        self._timeout = timeout_seconds
        self._max_tokens = max_tokens
        self._client = client

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from openai import AsyncOpenAI
        except ImportError as exc:
            raise RuntimeError(
                "openai is not installed; install the 'llm' extra "
                "(uv sync --extra llm) or leave FLOWMESH_LLM_ENABLED=false"
            ) from exc
        if not self._api_key:
            raise RuntimeError("FLOWMESH_LLM_API_KEY is not set")
        kwargs: dict[str, Any] = {"api_key": self._api_key}
        if self._base_url:
            kwargs["base_url"] = self._base_url
        self._client = AsyncOpenAI(**kwargs)
        return self._client

    async def explain(
        self,
        *,
        order_id: str,
        score: float,
        band: RiskBand,
        contributions: list[Contribution],
        features: FeatureVector,
    ) -> Rationale:
        prompt = _render_prompt(score=score, band=band, contributions=contributions)
        try:
            client = self._ensure_client()
            started = monotonic()
            async with self._metrics.timer(self._metrics.llm_latency):
                response = await asyncio.wait_for(
                    client.chat.completions.create(
                        model=self._model,
                        messages=[
                            {"role": "system", "content": self._SYSTEM},
                            {"role": "user", "content": prompt},
                        ],
                        max_tokens=self._max_tokens,
                        temperature=0.0,
                    ),
                    timeout=self._timeout,
                )
            usage = getattr(response, "usage", None)
            if usage is not None:
                self._metrics.llm_prompt_tokens.inc(int(getattr(usage, "prompt_tokens", 0) or 0))
                self._metrics.llm_completion_tokens.inc(
                    int(getattr(usage, "completion_tokens", 0) or 0)
                )
            self._metrics.llm_calls.labels(outcome="ok").inc()
            text = (response.choices[0].message.content or "").strip()
            if not text:
                raise ValueError("empty completion")
            logger.info(
                "llm rationale produced order_id=%s latency_ms=%.1f",
                order_id,
                (monotonic() - started) * 1000,
            )
            return Rationale(text=text, source="llm", llm_used=True)
        except Exception as exc:  # noqa: BLE001 - the fallback is the point
            self._metrics.llm_calls.labels(outcome="fallback").inc()
            logger.warning("llm rationale failed, using deterministic text: %s", exc)
            return await self._fallback.explain(
                order_id=order_id,
                score=score,
                band=band,
                contributions=contributions,
                features=features,
            )


def _render_prompt(*, score: float, band: RiskBand, contributions: list[Contribution]) -> str:
    """Build the user message.

    Only positive contributions, capped, and with the sign preserved. Sending the
    LLM the negative contributions as well invites it to explain a score *down*,
    which reads like mitigating evidence in a prompt that has no notion of
    mitigating.
    """
    drivers = [row for row in contributions if row.value > 0.0][:6]
    lines = "\n".join(
        f"- {row.feature}: {row.label} (value {row.value:.2f}, weight {row.weight:.2f}, "
        f"contribution {row.contribution:+.2f})"
        for row in drivers
    )
    return (
        f"score={score:.3f}\nband={band.value}\n"
        f"model contributions:\n{lines or '- (none above the reporting floor)'}\n\n"
        "Explain this score to a fraud analyst."
    )


def build_reasoner(
    *,
    llm_enabled: bool,
    metrics: FlowMeshMetrics,
    model: str = "gpt-4o-mini",
    api_key: str = "",
    base_url: str = "",
    timeout_seconds: float = 3.0,
    max_tokens: int = 200,
) -> LlmReasoner:
    """The wiring, in one place: deterministic always, LLM only if asked for."""
    deterministic: LlmReasoner = DeterministicReasoner()
    inner: LlmReasoner = deterministic
    if llm_enabled:
        inner = OpenAiReasoner(
            fallback=deterministic,
            metrics=metrics,
            model=model,
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            max_tokens=max_tokens,
        )
    return CachingReasoner(inner)


def reason_labels() -> dict[str, str]:
    """Expose the label map, for the dashboard's reason-code filter."""
    return dict(REASON_LABELS)
