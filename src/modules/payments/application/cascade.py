import asyncio
import logging
from collections.abc import Callable
from enum import StrEnum
from typing import Literal
from uuid import UUID

from modules.payments.application.errors import DuplicateProviderPaymentId
from modules.payments.application.ports import (
    CircuitBreaker,
    Clock,
    CreateProviderPayment,
    Metrics,
    NullMetrics,
    ProviderCreateResult,
    ProviderFailure,
    ProviderRegistry,
    UnitOfWorkFactory,
)
from modules.payments.application.routing import Router
from modules.payments.config import CascadeSettings
from modules.payments.domain.errors import AttemptFailure, FailureClass
from modules.payments.domain.policies import PENALIZED, Decision
from modules.payments.domain.statuses import PaymentStatus
from modules.payments.domain.value_objects import ProviderCode

log = logging.getLogger(__name__)


class StepResult(StrEnum):
    DONE = "DONE"
    NEXT_PROVIDER = "NEXT_PROVIDER"
    STOP = "STOP"
    NO_PROVIDER = "NO_PROVIDER"


_FROM_DECISION = {
    Decision.DONE: StepResult.DONE,
    Decision.NEXT_PROVIDER: StepResult.NEXT_PROVIDER,
    Decision.STOP: StepResult.STOP,
}


class CascadeOrchestrator:
    """Один шаг каскада = одна попытка у одного провайдера (§2.7).

    Попытка сохраняется ДО вызова провайдера, сам вызов идёт вне транзакции, результат
    применяется к агрегату под блокировкой строки. Одинаково используется синхронным
    API (цикл `run`) и фоновой задачей Taskiq.
    """

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        registry: ProviderRegistry,
        router: Router,
        breaker: CircuitBreaker,
        clock: Clock,
        settings: CascadeSettings,
        callback_url_for: Callable[[ProviderCode], str],
        metrics: Metrics | None = None,
    ) -> None:
        self._uow = uow_factory
        self._registry = registry
        self._router = router
        self._breaker = breaker
        self._clock = clock
        self._settings = settings
        self._callback_url_for = callback_url_for
        self._metrics = metrics or NullMetrics()
        self._bulkheads: dict[str, asyncio.Semaphore] = {}

    async def run(self, payment_id: UUID) -> Literal["completed", "deferred"]:
        deadline = self._clock.monotonic() + self._settings.deadline_s
        depth = 0
        while True:
            if deadline - self._clock.monotonic() < self._settings.min_step_s:
                log.info("cascade deferred", extra={"payment_id": str(payment_id)})
                return "deferred"
            result = await self.run_step(payment_id, deadline)
            depth += 1
            if result is not StepResult.NEXT_PROVIDER:
                self._metrics.cascade_depth(depth)
                return "completed"

    async def run_step(self, payment_id: UUID, deadline: float) -> StepResult:
        now = self._clock.now()
        async with self._uow() as uow:
            payment = await uow.payments.get_for_update(payment_id)
            if payment is None or payment.status not in (PaymentStatus.NEW, PaymentStatus.ROUTING):
                return StepResult.DONE
            if payment.active_attempt is not None:
                return StepResult.DONE  # шаг уже выполняет другой воркер
            code = await self._router.next(uow, payment)
            if code is None:
                payment.fail_no_provider(now)
                await uow.payments.save(payment)
                await uow.commit()
                return StepResult.NO_PROVIDER
            attempt = payment.start_attempt(code, now)
            await uow.payments.save(payment)
            await uow.commit()

        cmd = CreateProviderPayment(
            attempt_id=attempt.id,
            price=payment.price,
            pay_currency=payment.pay_currency,
            callback_url=self._callback_url_for(code),
            timeout_s=max(
                0.1, min(self._settings.read_timeout_s, deadline - self._clock.monotonic())
            ),
        )
        result, failure = await self._call_provider(code, cmd)
        try:
            decision = await self._apply_outcome(payment_id, attempt.id, result, failure)
        except DuplicateProviderPaymentId:
            # id провайдера уже занят другой попыткой — ответ провайдера некорректен
            log.error("duplicate provider_payment_id", extra={"provider": code})
            dup = AttemptFailure(cls=FailureClass.PROVIDER_ERROR, code="duplicate_provider_id")
            decision = await self._apply_outcome(payment_id, attempt.id, None, dup)
        return _FROM_DECISION[decision]

    async def _apply_outcome(
        self,
        payment_id: UUID,
        attempt_id: UUID,
        result: ProviderCreateResult | None,
        failure: AttemptFailure | None,
    ) -> Decision:
        async with self._uow() as uow:
            payment = await uow.payments.get_for_update(payment_id)
            assert payment is not None
            if result is not None:
                decision = payment.apply_create_success(
                    attempt_id, result.provider_payment_id, result.checkout, self._clock.now()
                )
            else:
                assert failure is not None
                decision = payment.apply_create_failure(attempt_id, failure, self._clock.now())
            await uow.payments.save(payment)
            await uow.commit()
        return decision

    async def _call_provider(
        self, code: ProviderCode, cmd: CreateProviderPayment
    ) -> tuple[ProviderCreateResult | None, AttemptFailure | None]:
        provider = self._registry.get(code)
        sem = self._bulkhead(code)
        started = self._clock.monotonic()
        try:
            await asyncio.wait_for(sem.acquire(), self._settings.bulkhead_wait_s)
        except TimeoutError:
            failure = AttemptFailure(cls=FailureClass.LOCAL_LIMIT, code="bulkhead")
            self._metrics.attempt(code, "failure", failure.cls)
            return None, failure
        try:
            result = await provider.create(cmd)
        except ProviderFailure as e:
            failure = e.failure
        except Exception as e:  # баг адаптера/неожиданный ответ — считаем ошибкой провайдера
            log.exception("unexpected provider error", extra={"provider": code})
            failure = AttemptFailure(
                cls=FailureClass.PROVIDER_ERROR, code=type(e).__name__, message=str(e)[:200]
            )
        else:
            self._metrics.attempt(code, "success", "")
            self._metrics.latency(code, "create", self._clock.monotonic() - started)
            await self._breaker.record(code, ok=True)
            return result, None
        finally:
            sem.release()

        self._metrics.attempt(code, "failure", failure.cls)
        self._metrics.latency(code, "create", self._clock.monotonic() - started)
        if failure.cls in PENALIZED:
            await self._breaker.record(code, ok=False)
        return None, failure

    def _bulkhead(self, code: str) -> asyncio.Semaphore:
        sem = self._bulkheads.get(code)
        if sem is None:
            sem = self._bulkheads[code] = asyncio.Semaphore(self._settings.bulkhead_per_provider)
        return sem
