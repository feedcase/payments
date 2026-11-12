from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, PrivateAttr

from modules.payments.domain.errors import AttemptFailure, DomainError, FailureClass
from modules.payments.domain.events import (
    DomainEvent,
    PaymentExpired,
    PaymentFailed,
    PaymentOverpaid,
    PaymentPartiallyPaid,
    PaymentSucceeded,
)
from modules.payments.domain.policies import Decision, decide_on_create_failure
from modules.payments.domain.statuses import (
    ACTIVE_ATTEMPT,
    AttemptStatus,
    PaymentStatus,
    can_transition,
)
from modules.payments.domain.value_objects import (
    Checkout,
    Money,
    PaymentMethod,
    ProviderCode,
    Purpose,
)

A = AttemptStatus
P = PaymentStatus

NO_PROVIDER_REASON = "no_provider_available"
_PAID = frozenset({A.FINISHED, A.PARTIALLY_PAID})
_TRIED = frozenset({A.CREATE_FAILED, A.CREATE_UNKNOWN, A.FAILED})


class PaymentAttempt(BaseModel):
    id: UUID
    seq: int
    round: int
    provider: ProviderCode
    status: AttemptStatus
    provider_payment_id: str | None = None
    failure: AttemptFailure | None = None
    checkout: Checkout | None = None
    received: Money | None = None
    created_at: datetime
    updated_at: datetime
    checked_at: datetime | None = None


class Payment(BaseModel):
    """Агрегат платежа. Все инварианты каскада проверяются здесь."""

    id: UUID
    user_id: UUID
    price: Money
    pay_currency: str
    purpose: Purpose
    method: PaymentMethod = PaymentMethod.CRYPTO
    status: PaymentStatus
    attempts: list[PaymentAttempt] = []
    received_total: Decimal = Decimal(0)
    round: int = 1
    idempotency_key: str
    request_hash: str
    failure_reason: str | None = None
    version: int = 0
    created_at: datetime
    updated_at: datetime

    _events: list[DomainEvent] = PrivateAttr(default_factory=list)

    # ---- factory -------------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        *,
        user_id: UUID,
        price: Money,
        pay_currency: str,
        purpose: Purpose,
        idempotency_key: str,
        request_hash: str,
        now: datetime,
    ) -> "Payment":
        return cls(
            id=uuid4(),
            user_id=user_id,
            price=price,
            pay_currency=pay_currency.lower(),
            purpose=purpose,
            status=P.NEW,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            created_at=now,
            updated_at=now,
        )

    # ---- queries -------------------------------------------------------------------------

    @property
    def active_attempt(self) -> PaymentAttempt | None:
        return next((a for a in self.attempts if a.status in ACTIVE_ATTEMPT), None)

    def attempt(self, attempt_id: UUID) -> PaymentAttempt:
        for a in self.attempts:
            if a.id == attempt_id:
                return a
        raise DomainError(f"attempt {attempt_id} not found in payment {self.id}")

    def tried_providers(self) -> set[ProviderCode]:
        """Провайдеры, которые уже не смогли обслужить текущий раунд."""
        return {
            a.provider
            for a in self.attempts
            if a.round == self.round
            and (a.status in _TRIED or (a.status is A.SUPERSEDED and a.failure is not None))
        }

    # ---- create phase --------------------------------------------------------------------

    def start_attempt(self, provider: ProviderCode, now: datetime) -> PaymentAttempt:
        if self.status not in (P.NEW, P.ROUTING):
            raise DomainError(f"cannot start attempt in status {self.status}")
        if self.active_attempt is not None:
            raise DomainError("payment already has an active attempt")
        for a in self.attempts:
            if a.status is A.CREATE_UNKNOWN:
                self._set_attempt(a, A.SUPERSEDED, now)
        attempt = PaymentAttempt(
            id=uuid4(),
            seq=len(self.attempts) + 1,
            round=self.round,
            provider=provider,
            status=A.CREATING,
            created_at=now,
            updated_at=now,
        )
        self.attempts.append(attempt)
        self._set_status(P.ROUTING, now)
        return attempt

    def apply_create_success(
        self, attempt_id: UUID, provider_payment_id: str, checkout: Checkout, now: datetime
    ) -> Decision:
        a = self.attempt(attempt_id)
        a.provider_payment_id = provider_payment_id
        a.checkout = checkout
        if self._superseded_while_creating(a):
            return Decision.DONE  # счёт записан, сверка увидит его, если покупатель заплатит
        self._set_attempt(a, A.CREATED, now)
        self._set_status(P.AWAITING_CUSTOMER, now)
        return Decision.DONE

    def apply_create_failure(
        self, attempt_id: UUID, failure: AttemptFailure, now: datetime
    ) -> Decision:
        a = self.attempt(attempt_id)
        if self._superseded_while_creating(a):
            a.failure = a.failure or failure
            return Decision.DONE
        a.failure = failure
        unknown = failure.cls is FailureClass.TIMEOUT_UNKNOWN
        self._set_attempt(a, A.CREATE_UNKNOWN if unknown else A.CREATE_FAILED, now)
        decision = decide_on_create_failure(failure.cls)
        if decision is Decision.STOP:
            self._fail(f"{failure.cls}:{failure.code}", now)
        return decision

    def orphan_attempt(self, attempt_id: UUID, now: datetime) -> None:
        """Попытка зависла в CREATING (процесс упал между транзакциями шага каскада).

        Исход у провайдера неизвестен — как при таймауте: для push-платежа это безопасно,
        покупатель адреса не видел. После этого каскад можно продолжить.
        """
        a = self.attempt(attempt_id)
        if a.status is not A.CREATING:
            return
        a.failure = AttemptFailure(cls=FailureClass.TIMEOUT_UNKNOWN, code="orphaned")
        self._set_attempt(a, A.CREATE_UNKNOWN, now)

    def fail_no_provider(self, now: datetime) -> None:
        self._fail(NO_PROVIDER_REASON, now)

    # ---- provider state (webhook / reconciliation) ---------------------------------------

    def apply_provider_state(
        self, attempt_id: UUID, status: AttemptStatus, received: Money | None, now: datetime
    ) -> Decision | None:
        a = self.attempt(attempt_id)
        if a.status == status or not can_transition(a.status, status):
            return None  # устаревшее или повторное состояние — статусы только вперёд
        was_active = a.status in ACTIVE_ATTEMPT
        self._set_attempt(a, status, now)
        if status in _PAID and received is not None:
            a.received = received
            self._recalc_received()

        if was_active:
            return self._on_active_attempt(a, status, now)
        return self._on_late_attempt(a, status, now)

    def _on_active_attempt(
        self, a: PaymentAttempt, status: AttemptStatus, now: datetime
    ) -> Decision:
        if status is A.CONFIRMING:
            self._set_status(P.CONFIRMING, now)
        elif status is A.PARTIALLY_PAID:
            self._set_status(P.PARTIALLY_PAID, now)
            self._emit(PaymentPartiallyPaid, attempt_id=a.id, received=a.received)
        elif status is A.FINISHED:
            self._succeed(a, now)
        elif status is A.EXPIRED:
            self._set_status(P.EXPIRED, now)
            self._emit(PaymentExpired, attempt_id=a.id)
        elif status is A.FAILED:
            a.failure = AttemptFailure(cls=FailureClass.CUSTOMER_FAILED, code="provider_failed")
            self._set_status(P.ROUTING, now)
            return Decision.NEXT_PROVIDER
        return Decision.DONE

    def _on_late_attempt(self, a: PaymentAttempt, status: AttemptStatus, now: datetime) -> Decision:
        """Деньги пришли по замещённой / истёкшей попытке — ничего не теряем."""
        if status not in _PAID:
            return Decision.DONE
        if self.status is P.SUCCEEDED:
            self._emit(PaymentOverpaid, attempt_id=a.id, provider=a.provider, received=a.received)
            return Decision.DONE
        if status is A.PARTIALLY_PAID:
            self._emit(PaymentPartiallyPaid, attempt_id=a.id, received=a.received)
            return Decision.DONE
        active = self.active_attempt
        if active is not None:
            self._set_attempt(active, A.SUPERSEDED, now)
        self._succeed(a, now)
        if active is not None and active.received is not None:
            # по замещённой попытке уже пришли деньги — это переплата, а не потеря
            self._emit(
                PaymentOverpaid,
                attempt_id=active.id,
                provider=active.provider,
                received=active.received,
            )
        return Decision.DONE

    def mark_checked(self, attempt_id: UUID, now: datetime) -> None:
        """Отметка сверки: провайдер опрошен, даже если статус не изменился."""
        self.attempt(attempt_id).checked_at = now

    # ---- reinvoice -----------------------------------------------------------------------

    def reinvoice(self, now: datetime) -> None:
        allowed = self.status is P.EXPIRED or (
            self.status is P.FAILED and self.failure_reason == NO_PROVIDER_REASON
        )
        if not allowed:
            raise DomainError(f"cannot reinvoice payment in status {self.status}")
        self.round += 1
        self.failure_reason = None
        self._set_status(P.ROUTING, now)

    # ---- events --------------------------------------------------------------------------

    def pull_events(self) -> list[DomainEvent]:
        events, self._events = self._events, []
        return events

    # ---- internals -----------------------------------------------------------------------

    @staticmethod
    def _superseded_while_creating(a: PaymentAttempt) -> bool:
        """Пока шёл вызов провайдера, попытку заместил опоздавший платёж по старой попытке
        или sweeper признал её зависшей. Исход только записываем — статус не трогаем."""
        return a.status is not A.CREATING

    def _succeed(self, a: PaymentAttempt, now: datetime) -> None:
        self._set_status(P.SUCCEEDED, now)
        self._emit(PaymentSucceeded, attempt_id=a.id, provider=a.provider, received=a.received)

    def _fail(self, reason: str, now: datetime) -> None:
        self.failure_reason = reason
        self._set_status(P.FAILED, now)
        self._emit(PaymentFailed, reason=reason)

    def _recalc_received(self) -> None:
        self.received_total = sum(
            (a.received.amount for a in self.attempts if a.received is not None),
            Decimal(0),
        )

    def _set_status(self, status: PaymentStatus, now: datetime) -> None:
        self.status = status
        self.updated_at = now

    @staticmethod
    def _set_attempt(a: PaymentAttempt, status: AttemptStatus, now: datetime) -> None:
        a.status = status
        a.updated_at = now

    def _emit(self, event_cls: type[DomainEvent], **fields: object) -> None:
        self._events.append(event_cls(payment_id=self.id, purpose=self.purpose, **fields))
