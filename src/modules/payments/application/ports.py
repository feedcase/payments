"""Порты application-слоя. Реализации — в infrastructure, фейки — в tests/fakes.py."""

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from types import TracebackType
from typing import Any, Protocol, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from modules.payments.domain.entities import Payment
from modules.payments.domain.errors import AttemptFailure
from modules.payments.domain.events import DomainEvent
from modules.payments.domain.statuses import AttemptStatus
from modules.payments.domain.value_objects import Checkout, Money, PaymentMethod, ProviderCode

# ---- провайдер ---------------------------------------------------------------------------


class ProviderFailure(Exception):
    def __init__(self, failure: AttemptFailure) -> None:
        super().__init__(f"{failure.cls}:{failure.code} {failure.message}".strip())
        self.failure = failure


class InvalidWebhook(Exception):
    pass


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class CreateProviderPayment(_Frozen):
    attempt_id: UUID
    price: Money
    pay_currency: str
    callback_url: str
    timeout_s: float


class ProviderCreateResult(_Frozen):
    provider_payment_id: str
    checkout: Checkout


class ProviderPaymentState(_Frozen):
    status: AttemptStatus
    received: Money | None = None


class ProviderEventRef(_Frozen):
    provider_payment_id: str | None
    order_id: UUID | None


class ProviderCapabilities(_Frozen):
    currencies: frozenset[str]
    min_amount: dict[str, Money] = {}


class PaymentProviderPort(Protocol):
    code: ProviderCode

    async def create(self, cmd: CreateProviderPayment) -> ProviderCreateResult: ...
    async def fetch(self, provider_payment_id: str) -> ProviderPaymentState | None: ...
    def parse_webhook(self, headers: Mapping[str, str], body: bytes) -> ProviderEventRef: ...
    async def capabilities(self, pay_currencies: Sequence[str]) -> ProviderCapabilities: ...
    async def health(self) -> bool: ...


class ProviderRegistry(Protocol):
    def get(self, code: ProviderCode) -> PaymentProviderPort: ...
    def has(self, code: ProviderCode) -> bool: ...
    def all(self) -> list[PaymentProviderPort]: ...


# ---- хранение ----------------------------------------------------------------------------


class PaymentRepository(Protocol):
    async def get(self, payment_id: UUID) -> Payment | None: ...
    async def get_for_update(self, payment_id: UUID) -> Payment | None: ...
    async def find_by_idempotency(self, user_id: UUID, key: str) -> Payment | None: ...
    async def add(self, payment: Payment) -> None: ...
    async def save(self, payment: Payment) -> None: ...
    async def find_attempt(
        self,
        provider: ProviderCode,
        provider_payment_id: str | None = None,
        attempt_id: UUID | None = None,
    ) -> tuple[UUID, UUID] | None: ...
    async def attempts_to_reconcile(
        self, now: datetime, active_after_s: int, closed_after_s: int, limit: int
    ) -> list[tuple[UUID, UUID]]: ...
    async def stuck_payments(self, now: datetime, older_than_s: int, limit: int) -> list[UUID]: ...
    async def attempts_past_expiry(
        self, now: datetime, grace_min: int, limit: int
    ) -> list[tuple[UUID, UUID]]: ...


class RouteRepository(Protocol):
    async def get(self, method: PaymentMethod, pay_currency: str) -> list[ProviderCode]: ...
    async def currencies(self) -> list[str]: ...


class InboxEvent(_Frozen):
    id: UUID
    provider: ProviderCode
    provider_payment_id: str | None
    order_id: UUID | None
    processed_at: datetime | None


class ProviderEventInbox(Protocol):
    async def add(
        self, provider: ProviderCode, event_hash: str, ref: ProviderEventRef, raw: str
    ) -> UUID | None: ...
    async def get(self, event_id: UUID) -> InboxEvent | None: ...
    async def mark_processed(self, event_id: UUID, note: str | None = None) -> None: ...
    async def unprocessed(self, now: datetime, older_than_s: int, limit: int) -> list[UUID]: ...


class OutboxRecord(_Frozen):
    id: UUID
    event_type: str
    payload: dict[str, Any]


class OutboxRepository(Protocol):
    async def add(self, event: DomainEvent) -> None: ...
    async def fetch_unpublished(self, batch: int) -> list[OutboxRecord]: ...
    async def mark_published(self, ids: Sequence[UUID]) -> None: ...


class UnitOfWork(Protocol):
    payments: PaymentRepository
    routes: RouteRepository
    events: ProviderEventInbox
    outbox: OutboxRepository

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    async def commit(self) -> None:
        """Фиксирует транзакцию; события сохранённых агрегатов уходят в outbox атомарно."""
        ...


UnitOfWorkFactory = Callable[[], UnitOfWork]


# ---- Redis-состояние ---------------------------------------------------------------------


class CircuitBreaker(Protocol):
    async def is_available(self, code: ProviderCode) -> bool: ...
    async def record(self, code: ProviderCode, ok: bool) -> None: ...


class CapabilityCache(Protocol):
    async def get(self, code: ProviderCode) -> ProviderCapabilities | None: ...
    async def set(self, code: ProviderCode, caps: ProviderCapabilities) -> None: ...


class IdempotencyLock(Protocol):
    async def acquire(self, user_id: UUID, key: str) -> bool: ...
    async def release(self, user_id: UUID, key: str) -> None: ...


class RateLimiter(Protocol):
    async def allow(self, user_id: UUID) -> bool: ...


# ---- транспорт / прочее ------------------------------------------------------------------


class CascadeDispatcher(Protocol):
    async def continue_later(self, payment_id: UUID) -> None: ...


class EventPublisher(Protocol):
    async def publish(self, event_type: str, payload: dict[str, Any]) -> None: ...


class Clock(Protocol):
    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...


class Metrics(Protocol):
    def attempt(self, provider: str, outcome: str, failure_class: str) -> None: ...
    def latency(self, provider: str, op: str, seconds: float) -> None: ...
    def cascade_depth(self, depth: int) -> None: ...


class NullMetrics:
    def attempt(self, provider: str, outcome: str, failure_class: str) -> None:
        pass

    def latency(self, provider: str, op: str, seconds: float) -> None:
        pass

    def cascade_depth(self, depth: int) -> None:
        pass
