"""Сборка модуля payments из настроек (composition root модуля)."""

from typing import Any
from uuid import UUID

import httpx
import redis.asyncio as aioredis

from app.db import make_engine, make_sessionmaker
from app.settings import Settings
from modules.payments.application.cascade import CascadeOrchestrator
from modules.payments.application.ports import UnitOfWork
from modules.payments.application.routing import Router
from modules.payments.application.use_cases import (
    ApplyProviderState,
    CreatePayment,
    ExpireStale,
    GetPayment,
    ProcessProviderEvent,
    Reconcile,
    RecoverStuck,
    RefreshCapabilities,
    Reinvoice,
    RelayOutbox,
)
from modules.payments.domain.value_objects import ProviderCode
from modules.payments.infrastructure.clock import SystemClock
from modules.payments.infrastructure.db.uow import SqlAlchemyUnitOfWork
from modules.payments.infrastructure.metrics import PrometheusMetrics
from modules.payments.infrastructure.providers.registry import build_registry
from modules.payments.infrastructure.redis.breaker import RedisCircuitBreaker
from modules.payments.infrastructure.redis.capabilities import RedisCapabilityCache
from modules.payments.infrastructure.redis.idempotency import (
    RedisIdempotencyLock,
    RedisRateLimiter,
)
from modules.payments.infrastructure.redis.publisher import RedisStreamPublisher
from modules.payments.infrastructure.task_queue import InlineTaskQueue, QueueDispatcher, TaskQueue
from modules.payments.infrastructure.taskiq.dispatcher import TaskiqTaskQueue
from modules.payments.presentation.deps import PaymentsFacade


class Container:
    def __init__(
        self,
        settings: Settings,
        *,
        http_transport: httpx.AsyncBaseTransport | None = None,
        task_queue: TaskQueue | None = None,
    ) -> None:
        self.settings = settings
        self.jwt = settings.jwt
        self._transport = http_transport
        self._queue_override = task_queue
        self._started = False

    def callback_url_for(self, code: ProviderCode) -> str:
        return f"{self.settings.public_base_url.rstrip('/')}/webhooks/{code}"

    async def startup(self) -> None:
        if self._started:
            return
        s = self.settings
        self.engine = make_engine(s.db.url, s.db.echo)
        self.sessionmaker = make_sessionmaker(self.engine)
        self.redis = aioredis.from_url(
            s.redis.url, decode_responses=True, socket_timeout=1.0, socket_connect_timeout=1.0
        )
        self.registry, self._clients = build_registry(
            cascade=s.cascade,
            nowpayments=s.nowpayments,
            coingate=s.coingate,
            mocks=s.mocks,
            transport=self._transport,
        )
        self.clock = SystemClock()
        self.breaker = RedisCircuitBreaker(self.redis, s.breaker)
        self.caps = RedisCapabilityCache(self.redis)
        self.publisher = RedisStreamPublisher(self.redis)

        def uow() -> UnitOfWork:
            return SqlAlchemyUnitOfWork(self.sessionmaker)

        self.uow_factory = uow
        self.cascade = CascadeOrchestrator(
            uow_factory=uow,
            registry=self.registry,
            router=Router(self.registry, self.caps, self.breaker),
            breaker=self.breaker,
            clock=self.clock,
            settings=s.cascade,
            callback_url_for=self.callback_url_for,
            metrics=PrometheusMetrics(),
        )
        self.queue: Any = self._queue_override or self._make_queue()
        self._owns_broker = False
        if isinstance(self.queue, TaskiqTaskQueue) and self._queue_override is None:
            from modules.payments.infrastructure.taskiq.broker import broker

            if not broker.is_worker_process:  # API-процесс: нужен только для .kiq()
                await broker.startup()
                self._owns_broker = True
        dispatcher = QueueDispatcher(self.queue)
        self.apply_state = ApplyProviderState(
            uow_factory=uow,
            registry=self.registry,
            cascade=self.cascade,
            dispatcher=dispatcher,
            clock=self.clock,
        )
        self.process_event = ProcessProviderEvent(uow_factory=uow, apply=self.apply_state)
        self.reconcile = Reconcile(
            uow_factory=uow, apply=self.apply_state, clock=self.clock, settings=s.cascade
        )
        self.expire_stale = ExpireStale(
            uow_factory=uow,
            registry=self.registry,
            apply=self.apply_state,
            clock=self.clock,
            settings=s.cascade,
        )
        self.refresh_capabilities = RefreshCapabilities(
            uow_factory=uow, registry=self.registry, caps=self.caps
        )
        self.relay_outbox = RelayOutbox(uow_factory=uow, publisher=self.publisher)
        self.recover = RecoverStuck(
            uow_factory=uow,
            cascade=self.cascade,
            process_event=self.process_event,
            clock=self.clock,
            settings=s.cascade,
        )
        self.facade = PaymentsFacade(
            create=CreatePayment(
                uow_factory=uow,
                cascade=self.cascade,
                dispatcher=dispatcher,
                lock=RedisIdempotencyLock(self.redis),
                rate_limiter=RedisRateLimiter(self.redis, s.rate_limit_per_min),
                clock=self.clock,
            ),
            get=GetPayment(uow_factory=uow),
            reinvoice=Reinvoice(
                uow_factory=uow, cascade=self.cascade, dispatcher=dispatcher, clock=self.clock
            ),
            registry=self.registry,
            uow_factory=uow,
            enqueue_provider_event=self.queue.process_provider_event,
        )
        self._started = True

    def _make_queue(self) -> TaskQueue:
        if self.settings.task_queue == "taskiq":
            return TaskiqTaskQueue()
        return self._inline_queue()

    def _inline_queue(self) -> TaskQueue:
        async def continue_cascade(pid: UUID) -> None:
            while await self.cascade.run(pid) == "deferred":
                pass

        async def process(eid: UUID) -> None:
            await self.process_event(eid)

        async def relay() -> None:
            await self.relay_outbox()

        return InlineTaskQueue(continue_cascade, process, relay)

    async def try_lock(self, name: str, ttl_s: int) -> bool:
        """Замок периодической задачи; при недоступном Redis — запускаем (идемпотентно)."""
        try:
            return bool(await self.redis.set(f"pay:lock:{name}", "1", nx=True, ex=ttl_s))
        except Exception:
            return True

    async def shutdown(self) -> None:
        if not self._started:
            return
        if isinstance(self.queue, InlineTaskQueue):
            await self.queue.drain()
        if self._owns_broker:
            from modules.payments.infrastructure.taskiq.broker import broker

            await broker.shutdown()
        for c in self._clients:
            await c.aclose()
        await self.redis.aclose()
        await self.engine.dispose()
        self._started = False
