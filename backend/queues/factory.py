"""Choosing a task-queue implementation.

Same three modes as `backend/events/factory.py`, for the same reason: one place
that answers "which broker is this process using", with a loud warning when
`auto` falls back. The fallback matters more here than for the event log,
because a task queue that silently became an in-process queue would lose review
requests -- and a lost review request is a fraudulent order that ships.
"""

from __future__ import annotations

from backend.config.settings import Settings
from backend.core.logging import get_logger
from backend.observability.metrics import FlowMeshMetrics
from backend.queues.memory_queue import InMemoryTaskQueue
from backend.queues.protocol import QueueUnavailableError, TaskQueue
from backend.queues.rabbitmq_queue import RabbitTaskQueue

logger = get_logger(__name__)


async def create_task_queue(settings: Settings, metrics: FlowMeshMetrics) -> TaskQueue:
    """Build and start the configured queue, declaring the topology."""
    mode = settings.queue_transport
    if mode == "memory":
        queue = InMemoryTaskQueue(metrics=metrics)
        await queue.start()
        logger.warning(
            "queue transport=memory: review requests live in this process only "
            "and are lost on exit. Set FLOWMESH_QUEUE_TRANSPORT=rabbitmq."
        )
        return queue

    rabbit = RabbitTaskQueue(
        url=settings.rabbitmq_url,
        metrics=metrics,
        prefetch=settings.rabbitmq_prefetch,
    )
    try:
        await rabbit.start()
    except QueueUnavailableError:
        if mode == "rabbitmq":
            raise
        logger.warning(
            "queue transport=auto: rabbitmq at %s unreachable, falling back to "
            "the in-process queue. Review requests will not survive a restart.",
            settings.rabbitmq_url,
        )
        queue = InMemoryTaskQueue(metrics=metrics)
        await queue.start()
        return queue

    await rabbit.declare(settings.review_queue)
    await rabbit.declare(settings.notification_queue)
    return rabbit


def is_in_memory(queue: TaskQueue) -> bool:
    """True when `queue` is the in-process implementation."""
    return isinstance(queue, InMemoryTaskQueue)
