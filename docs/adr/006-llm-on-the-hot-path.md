# ADR-006: an LLM cannot meet a 200ms p95, so it explains instead of scoring

**Status:** accepted
**Date:** 2026-09-29
**Context:** Sprint 7 — FlowMesh, fraud scoring on the order hot path

## The decision

The LLM reasoner is **off by default** and, when enabled, is invoked only for
scores in the *ambiguous* band, and only when the caller asked for a rationale.
Everything else — the fast path, the low and high bands — is scored by a
lightweight logistic model with no network call at all.

The reason is arithmetic, and it is worth doing explicitly rather than asserting:

| | latency |
| --- | --- |
| budget | 200ms p95 |
| logistic model + 12 features | ~0.06ms measured (`docs/load-test-report.md`) |
| smallest reasonable LLM call | 300ms best case, no upper bound on the tail |

An LLM on the hot path cannot meet a 200ms p95. Not "might not on a bad day" —
the *best case* is 50% over budget, and the tail is unbounded. That is not a
tuning problem, and no amount of prompt engineering moves a 300ms network round trip
under 200ms.

## So what is the LLM for?

Nuance the arithmetic cannot express. Concretely:

- **The model says 0.48.** That is a number, and it is not very informative. The
  contributions say `amount_zscore +0.31, country_mismatch +0.18`. A reviewer needs
  to know *which* of those is the actual problem, and whether the two together
  describe a plausible customer or a stolen card.
- **The deterministic reasoner** can say that. It is templates over the
  contributions: microseconds, no network, and it *cannot* hallucinate a reason the
  model did not use — which is the property that matters, because the explanation
  is shown to a human deciding whether to ship an order.
- **The LLM** can say that the combination is the signature of a card-testing
  burst, and suggest what the reviewer should check. That is worth 300ms *because a
  human is reading it*, not because it is on the path.

So the LLM is an **explainer for the uncertain middle**, not a scorer. The
distinction matters because the moment an LLM can move a score, the score stops
being auditable — and the score is the part with an audit trail.

## The band structure

```
score < 0.35          low        auto-approve, deterministic one-liner
0.35 <= score < 0.65  ambiguous  hold; reasoner may be consulted
score >= 0.65         high       hold; reasoner cannot overturn it
```

Two decisions in that diagram, both worth defending:

**1. The high band ignores the reasoner.** An LLM explanation attached to a high
score cannot lower it. If it could, "high" would mean "high, subject to a language
model's opinion", and the policy would not be auditable at all.

**2. The low band does not consult it either, even when asked.** No human is going
to read the explanation of an auto-approved order, so paying 300ms to generate it
would be latency spent on nothing. `rationale_worth_cost` requires *both* the
ambiguous band and an explicit request.

The thresholds are half-open at the low edge: a customer sitting exactly on 0.35 is
reviewed, not approved.

## What the LLM is structurally forbidden from doing

The system prompt says it, and the code enforces the part that matters:

- it receives the model's contributions, and is asked not to contradict them;
- it receives no ability to change the score — `OpenAiReasoner.explain()` returns a
  `Rationale`, and no caller reads a score from it;
- it is shown only the *positive* contributors. Sending it the negative ones
  invites it to explain a score *down*, which reads like mitigating evidence in a
  prompt that has no notion of mitigating.

`Rationale.source` is returned to the caller and logged, so a review screen can
show whether the text came from the model or from an LLM. A screen that shows one
while an auditor assumes the other is a small incident with a large paper trail.

## Failure behaviour

`OpenAiReasoner` fails **soft**: a timeout, a rate limit, a malformed response, or
the `openai` package not being installed all yield the deterministic rationale
instead. The alternative — failing the scoring call because an *explanation* was
unavailable — would push every LLM outage into the order pipeline, for a sentence
nobody is required to read.

Token counts are metered (`flowmesh_llm_prompt_tokens_total`,
`flowmesh_llm_completion_tokens_total`) so the cost of an explanation is a number
rather than an inference.

Rationales are cached on the feature vector, keyed by `(band, *feature_values)`.
Two ambiguous orders from the same kind of customer produce the same arithmetic and
therefore the same sentence, so the second is free. The band is part of the key: the
same vector on either side of a threshold gets a different sentence ("held" vs
"shipped"), and a key without the band would serve the wrong one.

The cache is bounded at 10,000 entries. An LLM rationale cache keyed on arbitrary
customer input is a memory-growth vector, and 10k × ~300 bytes covers the ambiguous
band of a busy minute.

## Measured

`backend/fraud/engine.py` measures latency around the whole scoring path, and
`tests/unit/fraud/test_engine.py` asserts:

- p95 over 200 sequential in-process scorings is under 200ms, and so is the max —
  the margin is deliberately generous, because the test catches an
  order-of-magnitude regression, not CI jitter
- `flowmesh_llm_calls_total{outcome="ok"}` is **0** after 20 clean orders and one
  obvious fraud, proving the fast path never reaches the reasoner
- 500 concurrent scorings complete well inside the budget per order, which is what
  catches a reasoner accidentally moving onto the fast path

The gRPC transport is where the network cost would show up, and it is measured
separately: `make loadtest` reports the in-process number and the report states
that it is in-process, so the two are never confused.

## Rejected alternatives

**A cheap classifier instead of a logistic model.** Faster and probably more
accurate. Rejected because the sprint's point is the pipeline around the decision,
and a model whose contributions you can read is one a fraud analyst can argue with
in a meeting. Swap the weights file for a real training run and nothing else
changes.

**A tree ensemble.** Better AUC, worse explanations, and an SHAP explainer at
inference time that is slower than the model. The same trade, with more moving parts.

**Calling the LLM on every order and accepting a slower p95.** Then the budget is
500ms and the resume line says 500ms, which is a materially weaker claim for a
system that also has to say "sub-200ms".

**Caching every score by customer.** Defeats the streaming features entirely — the
velocity and z-score signals are the ones worth having, and they change per order.