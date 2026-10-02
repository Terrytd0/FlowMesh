"""The RabbitMQ implementation of the task-queue port.

Topology, declared at startup so it is reviewable in one place:

```
review-queue        -- durable queue, x-dead-letter-exchange = review.dlx
review.dlx          -- direct exchange, the DLX
review.dlq          -- bound to review.dlx with routing key review
notification-queue  -- durable queue for customer/reviewer notifications
```

Two details that are not defaults and matter:

- **`x-dead-letter-exchange`** means a message nacked past the retry limit ends
  up somewhere an operator can look, instead of vanishing. A review request that
  disappears is a fraudulent order that nobody looks at.
- **Publisher confirms** (`await exchange.publish(...)` with confirms on the
  channel) mean `publish` returning implies the broker has the message. Without
  them, a dropped connection during publish is indistinguishable from a
  successful publish, and the review request is simply gone.

`aio-pika`'s `RobustChannel` reconnects with the queue re-declared on the new
connection; that is why this uses it rather than a bare channel.
"""

from __future__ import annotations

import asyncio
from typing import Any

from backend.core.logging import get_logger
from backend.observability.metrics import FlowMeshMetrics
from backend.queues.protocol import QueueUnavailableError, TaskHandler, record_depth

logger = get_logger(__name__)


class RabbitTaskQueue:
    """A `TaskQueue` backed by RabbitMQ."""

    def __init__(
        self,
        *,
        url: str,
        metrics: FlowMeshMetrics,
        prefetch: int = 32,
        dead_letter_suffix: str = ".dlq",
    ) -> None:
        self._url = url
        self._metrics = metrics
        self._prefetch = prefetch
        self._dead_letter_suffix = dead_letter_suffix
        self._connection: Any = None
        self._channel: Any = None
        self._started = False
        self._stopped = False

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        try:
            import aio_pika
        except ImportError as exc:  # pragma: no cover - dependency is declared
            raise QueueUnavailableError(f"aio-pika is not installed: {exc}") from exc
        try:
            self._connection = await aio_pika.connect_robust(self._url)
            self._channel = await self._connection.channel(publisher_confirms=True)
            await self._channel.set_qos(prefetch_count=self._prefetch)
        except Exception as exc:
            raise QueueUnavailableError(
                f"cannot reach rabbitmq at {self._url}: {type(exc).__name__}: {exc}"
            ) from exc
        self._started = True
        self._stopped = False
        logger.info("rabbitmq connected: %s", _redact(self._url))

    async def stop(self) -> None:
        self._stopped = True
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        self._started = False

    async def declare(self, queue: str) -> None:
        """Declare a durable queue with its dead-letter routing.

        Called on every start, and idempotent by design -- RabbitMQ treats a
        redeclaration with identical arguments as a no-op, and *inequally* as an
        error. That inequality is why the arguments are constructed in one place
        here instead of at each call site.
        """
        from aio_pika import ExchangeType

        assert self._channel is not None, "declare() before start()"
        dlx_name = f"{queue}.dlx"
        dead_letter = f"{queue}{self._dead_letter_suffix}"
        exchange = await self._channel.declare_exchange(dlx_name, ExchangeType.DIRECT, durable=True)
        await (await self._channel.declare_queue(dead_letter, durable=True)).bind(
            exchange, routing_key=queue
        )
        await self._channel.declare_queue(
            queue,
            durable=True,
            arguments={"x-dead-letter-exchange": dlx_name, "x-dead-letter-routing-key": queue},
        )

    # -- producing ---------------------------------------------------------

    async def publish(
        self, queue: str, body: bytes, *, headers: dict[str, Any] | None = None
    ) -> None:
        from aio_pika import DeliveryMode, Message

        if self._channel is None:
            raise QueueUnavailableError("rabbitmq channel is not open")
        await self.declare(queue)
        await self._channel.default_exchange.publish(
            Message(
                body,
                delivery_mode=DeliveryMode.PERSISTENT,
                headers=headers or {},
                content_type="application/json",
            ),
            routing_key=queue,
        )
        self._metrics.queue_published.labels(queue=queue).inc()

    # -- consuming ---------------------------------------------------------

    async def consume(self, queue: str, handler: TaskHandler, *, concurrency: int = 1) -> None:
        """Consume until `stop()`.

        `concurrency` spawns that many consumer tasks on the same channel. Each
        awaits its own ack, so the prefetch count -- not this number -- is what
        actually bounds in-flight messages; keeping both is deliberate so the
        relationship is visible.
        """
        if self._channel is None:
            raise QueueUnavailableError("rabbitmq channel is not open")
        await self.declare(queue)
        self._stopped = False
        consumer_queue = await self._channel.get_queue(queue)
        consumers = [
            asyncio.create_task(self._one(consumer_queue, handler), name=f"rabbit-{queue}-{slot}")
            for slot in range(max(1, concurrency))
        ]
        try:
            await asyncio.gather(*consumers)
        except asyncio.CancelledError:
            for consumer in consumers:
                consumer.cancel()
            raise

    async def _one(self, queue: Any, handler: TaskHandler) -> None:
        async with queue.iterator() as messages:
            while not self._stopped:
                message = await messages.__anext__()
                async with message.process(requeue=False, ignore_processed=True):
                    # `requeue=False` plus the queue's dead-letter exchange: a
                    # nack sends the message to the DLQ rather than bouncing it
                    # forever, which is the only version of this that a queue
                    # full of poison messages can recover from.
                    await handler(message.body)
                self._metrics.queue_processed.labels(queue=queue.name, outcome="approved").inc()

    # -- inspection --------------------------------------------------------

    async def depth(self, queue: str) -> dict[str, int]:
        if self._channel is None:
            raise QueueUnavailableError("rabbitmq channel is not open")
        await self.declare(queue)
        passive = await self._channel.declare_queue(queue, durable=True, passive=True)
        dead = f"{queue}{self._dead_letter_suffix}"
        depths = {
            "ready": passive.declaration_result.message_count or 0,
            "unacked": 0,
            "dead_letter": 0,
        }
        try:
            dead_queue = await self._channel.declare_queue(dead, durable=True, passive=True)
            depths["dead_letter"] = dead_queue.declaration_result.message_count or 0
        except Exception:  # noqa: BLE001 - a missing DLQ is depth zero, not an error
            pass
        record_depth(self._metrics, queue, depths)
        return depths


def _redact(url: str) -> str:
    """Hide the password in a broker URL before it reaches a log line."""
    if "@" not in url or "//" not in url:
        return url
    scheme, _, rest = url.partition("//")
    credentials, _, host = rest.rpartition("@")
    if ":" not in credentials:
        return url
    user, _, _password = credentials.partition(":")
    return f"{scheme}//{user}:***@{host}"
