"""Сценарии модуля payments. Единственная точка входа для presentation и задач Taskiq."""

import asyncio
import hashlib
import json
import logging
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from modules.payments.application.cascade import CascadeOrchestrator
from modules.payments.application.errors import (
    DuplicatePayment,
    IdempotencyMismatch,
    InvalidState,
    NotFound,
    RateLimited,
)
from modules.payments.application.ports import (
    CapabilityCache,
    CascadeDispatcher,
    Clock,
    EventPublisher,
    IdempotencyLock,
    ProviderPaymentState,
    ProviderRegistry,
    RateLimiter,
    UnitOfWorkFactory,
)
from modules.payments.config import CascadeSettings
from modules.payments.domain.entities import Payment, PaymentAttempt
from modules.payments.domain.errors import DomainError
from modules.payments.domain.policies import Decision
from modules.payments.domain.statuses import AttemptStatus, PaymentStatus
from modules.payments.domain.value_objects import Checkout, Money, Purpose

log = logging.getLogger(__name__)

LOCK_WAIT_S = 5.0
LOCK_POLL_S = 0.1


# ---- DTO ---------------------------------------------------------------------------------


class CreatePaymentCommand(BaseModel):
    user_id: UUID
    price: Money
    pay_currency: str
    purpose: Purpose
    idempotency_key: str


class AttemptView(BaseModel):
    attempt_id: UUID
    seq: int
    provider: str
    status: AttemptStatus
    checkout: Checkout | None


class PaymentView(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    price: Money
    pay_currency: str
    purpose: Purpose
    received_total: Decimal
    failure_reason: str | None
    attempt: AttemptView | None

    @classmethod
    def of(cls, p: Payment) -> "PaymentView":
        a = _shown_attempt(p)
        return cls(
            payment_id=p.id,
            status=p.status,
            price=p.price,
            pay_currency=p.pay_currency,
            purpose=p.purpose,
            received_total=p.received_total,
            failure_reason=p.failure_reason,
            attempt=None
            if a is None
            else AttemptView(
                attempt_id=a.id,
                seq=a.seq,
                provider=a.provider,
                status=a.status,
                checkout=a.checkout,
            ),
        )


def _shown_attempt(p: Payment) -> PaymentAttempt | None:
    """Что показать покупателю: активную попытку, иначе успешную, иначе последнюю."""
    if p.active_attempt is not None:
        return p.active_attempt
    finished = [a for a in p.attempts if a.status is AttemptStatus.FINISHED]
    if finished:
        return finished[0]
    return p.attempts[-1] if p.attempts else None


class CreatePaymentResult(BaseModel):
    view: PaymentView
    created: bool
    deferred: bool = False


def request_hash(cmd: CreatePaymentCommand) -> str:
    body = {
        "price": {"amount": str(cmd.price.amount), "currency": cmd.price.currency},
        "pay_currency": cmd.pay_currency.lower(),
        "purpose": cmd.purpose.model_dump(),
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


# ---- create / get / reinvoice ------------------------------------------------------------


async def _run_cascade(
    cascade: CascadeOrchestrator, dispatcher: CascadeDispatcher, payment_id: UUID
) -> bool:
    outcome = await cascade.run(payment_id)
    if outcome == "deferred":
        try:
            await dispatcher.continue_later(payment_id)
        except Exception:  # брокер недоступен — платёж подберёт RecoverStuck
            log.exception("cannot dispatch cascade", extra={"payment_id": str(payment_id)})
        return True
    return False


class CreatePayment:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        cascade: CascadeOrchestrator,
        dispatcher: CascadeDispatcher,
        lock: IdempotencyLock,
        rate_limiter: RateLimiter,
        clock: Clock,
    ) -> None:
        self._uow = uow_factory
        self._cascade = cascade
        self._dispatcher = dispatcher
        self._lock = lock
        self._limiter = rate_limiter
        self._clock = clock

    async def __call__(self, cmd: CreatePaymentCommand) -> CreatePaymentResult:
        h = request_hash(cmd)
        locked = await self._acquire(cmd.user_id, cmd.idempotency_key)
        try:
            existing = await self._existing(cmd, h)
            if existing is not None:
                return existing
            if not await self._allowed(cmd.user_id):
                raise RateLimited()
            payment = Payment.create(
                user_id=cmd.user_id,
                price=cmd.price,
                pay_currency=cmd.pay_currency,
                purpose=cmd.purpose,
                idempotency_key=cmd.idempotency_key,
                request_hash=h,
                now=self._clock.now(),
            )
            try:
                async with self._uow() as uow:
                    await uow.payments.add(payment)
                    await uow.commit()
            except DuplicatePayment:  # гонка без Redis-замка: unique в БД — последний рубеж
                existing = await self._existing(cmd, h)
                assert existing is not None
                return existing
            deferred = await _run_cascade(self._cascade, self._dispatcher, payment.id)
            async with self._uow() as uow:
                fresh = await uow.payments.get(payment.id)
            assert fresh is not None
            return CreatePaymentResult(view=PaymentView.of(fresh), created=True, deferred=deferred)
        finally:
            if locked:
                await self._release(cmd.user_id, cmd.idempotency_key)

    async def _existing(self, cmd: CreatePaymentCommand, h: str) -> CreatePaymentResult | None:
        async with self._uow() as uow:
            p = await uow.payments.find_by_idempotency(cmd.user_id, cmd.idempotency_key)
        if p is None:
            return None
        if p.request_hash != h:
            raise IdempotencyMismatch()
        return CreatePaymentResult(view=PaymentView.of(p), created=False)

    async def _acquire(self, user_id: UUID, key: str) -> bool:
        waited = 0.0
        while True:
            try:
                if await self._lock.acquire(user_id, key):
                    return True
            except Exception:
                log.warning("idempotency lock unavailable, relying on DB unique")
                return False
            if waited >= LOCK_WAIT_S:
                return False
            await asyncio.sleep(LOCK_POLL_S)
            waited += LOCK_POLL_S

    async def _release(self, user_id: UUID, key: str) -> None:
        try:
            await self._lock.release(user_id, key)
        except Exception:
            log.warning("idempotency lock release failed")

    async def _allowed(self, user_id: UUID) -> bool:
        try:
            return await self._limiter.allow(user_id)
        except Exception:
            log.warning("rate limiter unavailable, allowing")
            return True


class GetPayment:
    def __init__(self, *, uow_factory: UnitOfWorkFactory) -> None:
        self._uow = uow_factory

    async def __call__(self, user_id: UUID, payment_id: UUID) -> PaymentView:
        async with self._uow() as uow:
            p = await uow.payments.get(payment_id)
        if p is None or p.user_id != user_id:
            raise NotFound()
        return PaymentView.of(p)


class Reinvoice:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        cascade: CascadeOrchestrator,
        dispatcher: CascadeDispatcher,
        clock: Clock,
    ) -> None:
        self._uow = uow_factory
        self._cascade = cascade
        self._dispatcher = dispatcher
        self._clock = clock

    async def __call__(self, user_id: UUID, payment_id: UUID) -> CreatePaymentResult:
        async with self._uow() as uow:
            p = await uow.payments.get_for_update(payment_id)
            if p is None or p.user_id != user_id:
                raise NotFound()
            try:
                p.reinvoice(self._clock.now())
            except DomainError as e:
                raise InvalidState(str(e)) from e
            await uow.payments.save(p)
            await uow.commit()
        deferred = await _run_cascade(self._cascade, self._dispatcher, payment_id)
        async with self._uow() as uow:
            fresh = await uow.payments.get(payment_id)
        assert fresh is not None
        return CreatePaymentResult(view=PaymentView.of(fresh), created=True, deferred=deferred)


# ---- provider state: webhooks / reconciliation -------------------------------------------


class ApplyProviderState:
    """Перепроверяет статус попытки по API провайдера и применяет его к агрегату.

    Телу вебхука не доверяем: источник истины — ответ `fetch`.
    """

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        registry: ProviderRegistry,
        cascade: CascadeOrchestrator,
        dispatcher: CascadeDispatcher,
        clock: Clock,
    ) -> None:
        self._uow = uow_factory
        self._registry = registry
        self._cascade = cascade
        self._dispatcher = dispatcher
        self._clock = clock

    async def __call__(self, payment_id: UUID, attempt_id: UUID) -> None:
        state = await self.fetch(payment_id, attempt_id)
        await self.apply_state(payment_id, attempt_id, state)

    async def fetch(self, payment_id: UUID, attempt_id: UUID) -> ProviderPaymentState | None:
        async with self._uow() as uow:
            p = await uow.payments.get(payment_id)
        if p is None:
            return None
        a = p.attempt(attempt_id)
        if a.provider_payment_id is None or not self._registry.has(a.provider):
            return None
        return await self._registry.get(a.provider).fetch(a.provider_payment_id)

    async def apply_state(
        self, payment_id: UUID, attempt_id: UUID, state: ProviderPaymentState | None
    ) -> None:
        now = self._clock.now()
        decision: Decision | None = None
        async with self._uow() as uow:
            p = await uow.payments.get_for_update(payment_id)
            if p is None:
                return
            if state is not None:
                decision = p.apply_provider_state(attempt_id, state.status, state.received, now)
            p.mark_checked(attempt_id, now)
            await uow.payments.save(p)
            await uow.commit()
        if decision is Decision.NEXT_PROVIDER:
            await _run_cascade(self._cascade, self._dispatcher, payment_id)


class ProcessProviderEvent:
    def __init__(self, *, uow_factory: UnitOfWorkFactory, apply: ApplyProviderState) -> None:
        self._uow = uow_factory
        self._apply = apply

    async def __call__(self, event_id: UUID) -> None:
        async with self._uow() as uow:
            ev = await uow.events.get(event_id)
            if ev is None or ev.processed_at is not None:
                return
            found = None
            if ev.provider_payment_id:
                found = await uow.payments.find_attempt(
                    ev.provider, provider_payment_id=ev.provider_payment_id
                )
            if found is None and ev.order_id:
                found = await uow.payments.find_attempt(ev.provider, attempt_id=ev.order_id)
            if found is None:
                await uow.events.mark_processed(event_id, note="unknown_attempt")
                await uow.commit()
                log.warning("webhook for unknown attempt", extra={"event_id": str(event_id)})
                return
        await self._apply(*found)
        async with self._uow() as uow:
            await uow.events.mark_processed(event_id)
            await uow.commit()


class Reconcile:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        apply: ApplyProviderState,
        clock: Clock,
        settings: CascadeSettings,
    ) -> None:
        self._uow = uow_factory
        self._apply = apply
        self._clock = clock
        self._settings = settings

    async def __call__(self, limit: int = 100) -> int:
        s = self._settings
        async with self._uow() as uow:
            items = await uow.payments.attempts_to_reconcile(
                self._clock.now(), s.reconcile_after_s, s.reconcile_closed_after_s, limit
            )
        sem = asyncio.Semaphore(s.reconcile_concurrency)

        async def one(payment_id: UUID, attempt_id: UUID) -> bool:
            async with sem:
                try:
                    await self._apply(payment_id, attempt_id)
                    return True
                except Exception:
                    log.exception("reconcile failed", extra={"attempt_id": str(attempt_id)})
                    return False

        results = await asyncio.gather(*(one(p, a) for p, a in items))
        return sum(results)


class RecoverStuck:
    """Подбирает то, что потерялось между процессами (§2.11 «сверка подберёт зависшее»).

    - NEW/ROUTING платежи без движения дольше stuck_after_s: попытка, зависшая в CREATING
      (процесс упал между транзакциями шага), помечается CREATE_UNKNOWN, каскад продолжается;
    - вебхуки, сохранённые в inbox, но не обработанные (очередь была недоступна).
    """

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        cascade: CascadeOrchestrator,
        process_event: "ProcessProviderEvent",
        clock: Clock,
        settings: CascadeSettings,
    ) -> None:
        self._uow = uow_factory
        self._cascade = cascade
        self._process = process_event
        self._clock = clock
        self._settings = settings

    async def __call__(self, limit: int = 100) -> int:
        now, s = self._clock.now(), self._settings
        async with self._uow() as uow:
            payments = await uow.payments.stuck_payments(now, s.stuck_after_s, limit)
            events = await uow.events.unprocessed(now, s.inbox_retry_after_s, limit)
        recovered = 0
        for payment_id in payments:
            try:
                await self._recover_payment(payment_id)
                recovered += 1
            except Exception:
                log.exception("recover failed", extra={"payment_id": str(payment_id)})
        for event_id in events:
            try:
                await self._process(event_id)
                recovered += 1
            except Exception:
                log.exception("inbox retry failed", extra={"event_id": str(event_id)})
        return recovered

    async def _recover_payment(self, payment_id: UUID) -> None:
        async with self._uow() as uow:
            p = await uow.payments.get_for_update(payment_id)
            if p is None or p.status not in (PaymentStatus.NEW, PaymentStatus.ROUTING):
                return
            for a in p.attempts:
                if a.status is AttemptStatus.CREATING:
                    log.warning("orphaned attempt", extra={"attempt_id": str(a.id)})
                    p.orphan_attempt(a.id, self._clock.now())
            await uow.payments.save(p)
            await uow.commit()
        while await self._cascade.run(payment_id) == "deferred":
            pass


class ExpireStale:
    """Закрывает счета, которые провайдер так и не перевёл из ожидания после expires_at."""

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        registry: ProviderRegistry,
        apply: ApplyProviderState,
        clock: Clock,
        settings: CascadeSettings,
    ) -> None:
        self._uow = uow_factory
        self._registry = registry
        self._apply = apply
        self._clock = clock
        self._settings = settings

    async def __call__(self, limit: int = 100) -> int:
        async with self._uow() as uow:
            items = await uow.payments.attempts_past_expiry(
                self._clock.now(), self._settings.expire_grace_min, limit
            )
        expired = 0
        for payment_id, attempt_id in items:
            try:
                state = await self._apply.fetch(payment_id, attempt_id)
                if state is None or state.status is AttemptStatus.CREATED:
                    state = ProviderPaymentState(status=AttemptStatus.EXPIRED)
                    expired += 1
                await self._apply.apply_state(payment_id, attempt_id, state)
            except Exception:
                log.exception("expire failed", extra={"attempt_id": str(attempt_id)})
        return expired


# ---- capabilities / outbox ---------------------------------------------------------------


class RefreshCapabilities:
    def __init__(
        self, *, uow_factory: UnitOfWorkFactory, registry: ProviderRegistry, caps: CapabilityCache
    ) -> None:
        self._uow = uow_factory
        self._registry = registry
        self._caps = caps

    async def __call__(self) -> int:
        async with self._uow() as uow:
            currencies = await uow.routes.currencies()
        ok = 0
        for provider in self._registry.all():
            try:
                await self._caps.set(provider.code, await provider.capabilities(currencies))
                ok += 1
            except Exception:
                log.exception("capabilities refresh failed", extra={"provider": provider.code})
        return ok


class RelayOutbox:
    def __init__(self, *, uow_factory: UnitOfWorkFactory, publisher: EventPublisher) -> None:
        self._uow = uow_factory
        self._publisher = publisher

    async def __call__(self, batch: int = 100) -> int:
        async with self._uow() as uow:
            records = await uow.outbox.fetch_unpublished(batch)
            published = []
            try:
                for r in records:
                    await self._publisher.publish(r.event_type, r.payload)
                    published.append(r.id)
            finally:
                if published:
                    await uow.outbox.mark_published(published)
                    await uow.commit()
        return len(published)
