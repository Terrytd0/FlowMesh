"""In-process task queue with real ack/nack/dead-letter semantics.

The in-memory counterpart to the RabbitMQ port, and like the in-process event
log it is an implementation rather than a stub: messages are held until acked,
a nack requeues (or dead-letters, past the limit), prefetch is honoured, and
`depth()` reports the three states separately.

What it does *not* model, and why that is safe for this pipeline:

- **Per-message TTL expiry.** The review SLA is a business deadline checked by
  the worker's own query (`oldest item age`), not a broker TTL. Modelling TTL
  here would create a second, divergent implementation of the same policy.
- **Routing keys.** This pipeline publishes directly to named queues, so a
  topic exchange buys nothing.
- **Publisher confirms.** The in-memory publish is synchronous, which is
  *stronger* than a confirmed publish. The RabbitMQ path asserts its own
  confirms, so the weaker real path is the one that is tested against the
  stronger contract rather than the other way round.

RabbitMQ *does* have TTLs and exchanges configured for it (see
`backend/queues/rabbitmq_queue.py`) because a real deployment wants them; this
implementation simply does not pretend to be a broker.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

from backend.core.logging import get_logger
from backend.observability.metrics import FlowMeshMetrics
from backend.queues.protocol import TaskHandler, record_depth

logger = get_logger(__name__)

_IDLE_SLEEP_SECONDS = 0.002


class InMemoryTaskQueue:
    """A `TaskQueue` backed by this process's memory."""

    def __init__(
        self, *, metrics: FlowMeshMetrics, max_attempts: int = 3, dead_letter_suffix: str = ".dlq"
    ) -> None:
        self._metrics = metrics
        self._max_attempts = max_attempts
        self._dead_letter_suffix = dead_letter_suffix
        self._ready: dict[str, deque[tuple[bytes, dict[str, str], int]]] = {}
        self._unacked: dict[str, list[tuple[bytes, dict[str, str], int]]] = {}
        self._dead_letter: dict[str, list[bytes]] = {}
        self._started = False
        self._stopped = False

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        self._started = True
        self._stopped = False

    async def stop(self) -> None:
        self._stopped = True
        await asyncio.sleep(0)

    # -- producing ---------------------------------------------------------

    async def publish(
        self, queue: str, body: bytes, *, headers: dict[str, str] | None = None
    ) -> None:
        self._ready.setdefault(queue, deque()).append((body, headers or {}, 1))
        self._metrics.queue_published.labels(queue=queue).inc()
        self._publish_depth(queue)

    # -- consuming ---------------------------------------------------------

    async def consume(self, queue: str, handler: TaskHandler, *, concurrency: int = 1) -> None:
        """Run until `stop()`.

        `concurrency` prefetches at most that many messages at once, so the
        prefetch semantics that make RabbitMQ's redelivery behaviour
        non-obvious are actually exercised rather than assumed.
        """
        self._ready.setdefault(queue, deque())
        self._unacked.setdefault(queue, [])
        self._stopped = False
        workers = [
            asyncio.create_task(self._worker(queue, handler, slot), name=f"{queue}-w{slot}")
            for slot in range(max(1, concurrency))
        ]
        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            for worker in workers:
                worker.cancel()
            raise

    async def _worker(self, queue: str, handler: TaskHandler, _slot: int) -> None:
        while not self._stopped:
            pending = self._ready.get(queue)
            if not pending:
                await asyncio.sleep(_IDLE_SLEEP_SECONDS)
                continue
            body, headers, attempts = pending.popleft()
            self._unacked.setdefault(queue, []).append((body, headers, attempts))
            self._publish_depth(queue)
            try:
                await handler(body)
            except Exception as exc:
                self._unacked[queue].pop()
                if attempts >= self._max_attempts:
                    # Keyed by the *suffixed* name, matching where `depth()` and
                    # `dead_letters()` read it. The two used different keys, so a
                    # dead-lettered message was recorded under one name and counted
                    # under another -- `depth()["dead_letter"]` read 0 while the
                    # counter said 1, and the queue looked empty to an operator
                    # during exactly the incident where it was not.
                    self._dead_letter.setdefault(self._dead_letter_key(queue), []).append(body)
                    self._metrics.queue_dead_lettered.labels(queue=queue).inc()
                    logger.error(
                        "task dead-lettered queue=%s attempts=%s error=%s",
                        queue,
                        attempts,
                        f"{type(exc).__name__}: {exc}",
                    )
                else:
                    # Requeue at the *back*, not the front. Appending to the left
                    # would starve every task behind a poison message until the
                    # attempt limit was reached -- which is the failure the dead
                    # letter is supposed to bound, not create.
                    pending.append((body, headers, attempts + 1))
                self._metrics.queue_processed.labels(queue=queue, outcome="error").inc()
                self._publish_depth(queue)
                continue
            self._unacked[queue].pop()
            self._metrics.queue_processed.labels(queue=queue, outcome="approved").inc()
            self._publish_depth(queue)

    # -- inspection --------------------------------------------------------

    def _dead_letter_key(self, queue: str) -> str:
        """The dead-letter queue's name.

        One function, because the writer and the two readers each spelled this
        out separately and one of them spelled it differently -- a dead-lettered
        message that the gauge reported as zero.
        """
        return f"{queue}{self._dead_letter_suffix}"

    async def depth(self, queue: str) -> dict[str, int]:
        return {
            "ready": len(self._ready.get(queue, ())),
            "unacked": len(self._unacked.get(queue, [])),
            "dead_letter": len(self._dead_letter.get(self._dead_letter_key(queue), [])),
        }

    def _publish_depth(self, queue: str) -> None:
        record_depth(self._metrics, queue, self._depths(queue))

    def _depths(self, queue: str) -> dict[str, int]:
        return {
            "ready": len(self._ready.get(queue, ())),
            "unacked": len(self._unacked.get(queue, [])),
            "dead_letter": len(self._dead_letter.get(self._dead_letter_key(queue), [])),
        }

    async def dead_letters(self, queue: str) -> list[bytes]:
        """Everything that ended up in the dead-letter queue, for tests and reports."""
        return list(self._dead_letter.get(self._dead_letter_key(queue), []))


def json_task(payload: Any) -> bytes:
    """Serialise a task body the same way both brokers' handlers read it."""
    import json

    return json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")


def parse_task(body: bytes) -> dict[str, Any]:
    """Parse a task body, raising a ValueError a handler can nack on."""
    import json

    return dict(json.loads(body.decode("utf-8")))
