"""Очередь фоновых шагов модуля. Inline-вариант для dev/тестов; Taskiq — в taskiq/."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

log = logging.getLogger(__name__)


class TaskQueue(Protocol):
    async def continue_cascade(self, payment_id: UUID) -> None: ...
    async def process_provider_event(self, event_id: UUID) -> None: ...
    async def relay_outbox(self) -> None: ...


class QueueDispatcher:
    """CascadeDispatcher поверх TaskQueue."""

    def __init__(self, queue: TaskQueue) -> None:
        self._q = queue

    async def continue_later(self, payment_id: UUID) -> None:
        await self._q.continue_cascade(payment_id)


class InlineTaskQueue:
    def __init__(
        self,
        continue_cascade: Callable[[UUID], Awaitable[object]],
        process_event: Callable[[UUID], Awaitable[object]],
        relay_outbox: Callable[[], Awaitable[object]],
    ) -> None:
        self._cc = continue_cascade
        self._pe = process_event
        self._ro = relay_outbox
        self._tasks: set[asyncio.Task[None]] = set()

    def _spawn(self, name: str, coro: Awaitable[object]) -> None:
        async def run() -> None:
            try:
                await coro
            except Exception:
                log.exception("inline task %s failed", name)

        t = asyncio.create_task(run())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def continue_cascade(self, payment_id: UUID) -> None:
        self._spawn("continue_cascade", self._cc(payment_id))

    async def process_provider_event(self, event_id: UUID) -> None:
        self._spawn("process_provider_event", self._pe(event_id))

    async def relay_outbox(self) -> None:
        self._spawn("relay_outbox", self._ro())

    async def drain(self) -> None:
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
