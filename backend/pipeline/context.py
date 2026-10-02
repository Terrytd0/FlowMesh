"""Pipeline wiring: the shared dependencies a consumer or worker needs.

`OrderProcessor`, `InventoryWorker` and `ReviewWorker` are written against their
own small ports, and this is the module that binds them to concrete objects. The
point of doing it in one place is that **there is exactly one answer to "what
does a handler get"**, so a change to the session scope or the inventory factory
is a one-file change rather than a hunt through four constructors.

The rule it enforces: every component that touches the database receives an
`async_sessionmaker`, never a session. Sessions are opened *inside* handlers, one
per event or task, and closed when the handler returns. A shared long-lived
session is how a pipeline ends up with one event's uncommitted writes visible to
another handler, and it is invisible in tests that only ever process one event at
a time.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.config.settings import Settings
from backend.core.logging import get_logger
from backend.observability.metrics import FlowMeshMetrics
from backend.queues.protocol import TaskQueue

logger = get_logger(__name__)


@dataclass(frozen=True)
class PipelineContext:
    """Everything the consumers and workers need, bound once at process start.

    Built by `backend/scripts/run_*.py`, held for the life of the process, and
    passed to each component. Immutable because every field is a client or a
    factory -- there is no state here to mutate, and a mutable one would tempt a
    handler into stashing something on it.
    """

    settings: Settings
    metrics: FlowMeshMetrics
    event_bus: Any
    task_queue: TaskQueue
    scorer: Any
    session_factory: async_sessionmaker[AsyncSession]

    def inventory_service(self, session: AsyncSession) -> Any:
        """A `InventoryService` bound to one caller's session.

        A factory, not a singleton. The service holds a session, and a session is
        per-transaction -- sharing one would put two handlers in one transaction
        and, on PostgreSQL, hold a row lock across a gRPC call.
        """
        from backend.inventory.service import InventoryService

        return InventoryService(
            session=session,
            metrics=self.metrics,
            ttl_seconds=self.settings.inventory_reservation_ttl_seconds,
        )


def build_order_processor(context: PipelineContext) -> Any:
    """The order processor, wired to this context."""
    from backend.pipeline.order_processor import OrderProcessor

    return OrderProcessor(
        settings=context.settings,
        bus=context.event_bus,
        queue=context.task_queue,
        scorer=context.scorer,
        metrics=context.metrics,
        session_factory=context.session_factory,
        inventory_service_factory=context.inventory_service,
    )


def build_inventory_worker(context: PipelineContext) -> Any:
    """The inventory worker, wired to this context."""
    from backend.pipeline.inventory_worker import InventoryWorker

    return InventoryWorker(
        settings=context.settings,
        bus=context.event_bus,
        metrics=context.metrics,
        session_factory=context.session_factory,
        inventory_service_factory=context.inventory_service,
    )


def build_review_worker(context: PipelineContext, *, actor: str = "worker:review") -> Any:
    """The review worker, wired to this context.

    `actor` is a parameter rather than a constant because the same worker class
    serves two actors: a human decision (`worker:review`) and the API's decision
    request (`api:supervisor`). The audit trail has to say which, or "who approved
    this order" has no answer.
    """
    from backend.pipeline.review_worker import ReviewWorker

    return ReviewWorker(
        settings=context.settings,
        queue=context.task_queue,
        bus=context.event_bus,
        metrics=context.metrics,
        session_factory=context.session_factory,
        inventory_service_factory=context.inventory_service,
        actor=actor,
    )
