from modules.payments.application.ports import (
    CapabilityCache,
    CircuitBreaker,
    ProviderRegistry,
    UnitOfWork,
)
from modules.payments.domain.entities import Payment
from modules.payments.domain.value_objects import ProviderCode


class Router:
    """Выбор следующего провайдера (§2.5): маршрут → capabilities → исключение испробованных
    → breaker. Breaker проверяется последним: в HALF_OPEN проверка расходует пробный вызов."""

    def __init__(
        self, registry: ProviderRegistry, caps: CapabilityCache, breaker: CircuitBreaker
    ) -> None:
        self._registry = registry
        self._caps = caps
        self._breaker = breaker

    async def next(self, uow: UnitOfWork, payment: Payment) -> ProviderCode | None:
        candidates = await uow.routes.get(payment.method, payment.pay_currency)
        tried = payment.tried_providers()
        for code in candidates:
            if code in tried or not self._registry.has(code):
                continue
            if not await self._supports(code, payment):
                continue
            if not await self._breaker.is_available(code):
                continue
            return code
        return None

    async def _supports(self, code: ProviderCode, payment: Payment) -> bool:
        caps = await self._caps.get(code)
        if caps is None:  # промах кэша: лучше спросить провайдера, чем отбросить его
            return True
        if payment.pay_currency not in caps.currencies:
            return False
        minimum = caps.min_amount.get(payment.pay_currency)
        return not (
            minimum is not None
            and minimum.currency == payment.price.currency
            and payment.price.amount < minimum.amount
        )
