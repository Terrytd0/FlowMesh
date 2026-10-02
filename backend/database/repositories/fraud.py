"""Fraud-score persistence.

Append-only, one row per decision. A re-scored order gets a second row.

That is not laziness -- it is the answer to the question asked after every fraud
incident: "what did the system decide at 14:03, and was that right?" A single
mutable row cannot answer it. `event_id` is unique, which is what makes the
write idempotent against a redelivered `order.scored` event without a separate
existence check.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.core.clock import utcnow
from backend.database.models import FraudScore
from backend.events.schema import RiskBand


async def insert_score(
    session: AsyncSession,
    *,
    event_id: str,
    order_id: str,
    score: float,
    band: RiskBand,
    model_version: str,
    reasons: list[str],
    rationale: str,
    contributions: dict[str, Any],
    degraded: bool,
    llm_used: bool,
    transport: str,
    latency_ms: float,
) -> bool:
    """Persist one decision. `False` means this event was already recorded."""
    session.add(
        FraudScore(
            event_id=event_id,
            order_id=order_id,
            score=score,
            band=band,
            model_version=model_version,
            reasons=list(reasons),
            rationale=rationale,
            contributions=contributions,
            degraded=degraded,
            llm_used=llm_used,
            transport=transport,
            latency_ms=latency_ms,
            decided_at=utcnow(),
        )
    )
    try:
        await session.flush()
    except IntegrityError:
        # The unique `event_id` did its job: this exact scoring event was already
        # recorded. A duplicate, not an error -- the handler returns cleanly and
        # the offset commits.
        return False
    return True


async def latest_score(session: AsyncSession, order_id: str) -> FraudScore | None:
    """Most recent decision for an order, or None."""
    result = await session.execute(
        select(FraudScore)
        .where(FraudScore.order_id == order_id)
        .order_by(FraudScore.decided_at.desc(), FraudScore.id.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def scores_for_order(session: AsyncSession, order_id: str) -> list[FraudScore]:
    result = await session.execute(
        select(FraudScore)
        .where(FraudScore.order_id == order_id)
        .order_by(FraudScore.decided_at, FraudScore.id)
    )
    return list(result.scalars())


async def fraud_rate(session: AsyncSession) -> dict[str, Any]:
    """Fraud rate and band distribution, for `/metrics` and the load report.

    Computed from the database rather than kept as a running counter: the counter
    and the table can disagree (a rollback leaves the counter ahead), and when
    they do, the number an auditor reads is the one in the table.
    """
    total = int(
        (await session.execute(select(func.count()).select_from(FraudScore))).scalar_one()  # type: ignore[arg-type]
    )
    rows = await session.execute(
        select(FraudScore.band, func.count()).group_by(FraudScore.band)  # type: ignore[misc]
    )
    by_band = {str(band): int(count) for band, count in rows.all()}
    mean = (
        float(
            (
                await session.execute(select(func.coalesce(func.avg(FraudScore.score), 0.0)))
            ).scalar_one()  # type: ignore[arg-type]
        )
        if total
        else 0.0
    )
    return {
        "total": total,
        "by_band": by_band,
        "mean_score": mean,
        "fraud_rate": by_band.get("high", 0) / total if total else 0.0,
    }
