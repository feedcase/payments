from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from modules.payments.domain.value_objects import Money, Purpose


class DomainEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payment_id: UUID
    purpose: Purpose

    @property
    def event_type(self) -> str:
        return type(self).__name__


class PaymentSucceeded(DomainEvent):
    attempt_id: UUID
    provider: str
    received: Money | None = None


class PaymentFailed(DomainEvent):
    reason: str


class PaymentPartiallyPaid(DomainEvent):
    attempt_id: UUID
    received: Money | None = None


class PaymentExpired(DomainEvent):
    attempt_id: UUID


class PaymentOverpaid(DomainEvent):
    attempt_id: UUID
    provider: str
    received: Money | None = None
