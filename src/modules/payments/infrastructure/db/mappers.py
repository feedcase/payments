from decimal import Decimal
from typing import Any

from modules.payments.domain.entities import Payment, PaymentAttempt
from modules.payments.domain.errors import AttemptFailure, FailureClass
from modules.payments.domain.statuses import AttemptStatus, PaymentStatus
from modules.payments.domain.value_objects import (
    Checkout,
    Money,
    PaymentMethod,
    ProviderCode,
    Purpose,
)
from modules.payments.infrastructure.db.models import AttemptRow, PaymentRow


def clean(d: Decimal) -> Decimal:
    """NUMERIC(36,18) отдаёт 18 знаков: 9.990000000000000000 → 9.99, 10.000… → 10."""
    n = d.normalize()
    return n.quantize(Decimal(1)) if n.as_tuple().exponent > 0 else n  # type: ignore[operator]


def payment_values(p: Payment) -> dict[str, Any]:
    return {
        "id": p.id,
        "user_id": p.user_id,
        "price_amount": p.price.amount,
        "price_currency": p.price.currency,
        "pay_currency": p.pay_currency,
        "method": p.method.value,
        "purpose_domain": p.purpose.domain,
        "purpose_ref": p.purpose.ref,
        "status": p.status.value,
        "received_total": p.received_total,
        "round": p.round,
        "idempotency_key": p.idempotency_key,
        "request_hash": p.request_hash,
        "failure_reason": p.failure_reason,
        "version": p.version,
        "created_at": p.created_at,
        "updated_at": p.updated_at,
    }


def attempt_values(payment_id: Any, a: PaymentAttempt) -> dict[str, Any]:
    return {
        "id": a.id,
        "payment_id": payment_id,
        "seq": a.seq,
        "round": a.round,
        "provider": a.provider,
        "provider_payment_id": a.provider_payment_id,
        "status": a.status.value,
        "failure_class": a.failure.cls.value if a.failure else None,
        "failure_code": a.failure.code if a.failure else None,
        "failure_message": a.failure.message if a.failure else None,
        "checkout": a.checkout.model_dump(mode="json") if a.checkout else None,
        "expires_at": a.checkout.expires_at if a.checkout else None,
        "received_amount": a.received.amount if a.received else None,
        "received_currency": a.received.currency if a.received else None,
        "created_at": a.created_at,
        "updated_at": a.updated_at,
        "checked_at": a.checked_at,
    }


def to_attempt(r: AttemptRow) -> PaymentAttempt:
    return PaymentAttempt(
        id=r.id,
        seq=r.seq,
        round=r.round,
        provider=ProviderCode(r.provider),
        status=AttemptStatus(r.status),
        provider_payment_id=r.provider_payment_id,
        failure=AttemptFailure(
            cls=FailureClass(r.failure_class),
            code=r.failure_code or "",
            message=r.failure_message or "",
        )
        if r.failure_class
        else None,
        checkout=Checkout.model_validate(r.checkout) if r.checkout else None,
        received=Money(amount=clean(r.received_amount), currency=r.received_currency)
        if r.received_amount is not None and r.received_currency
        else None,
        created_at=r.created_at,
        updated_at=r.updated_at,
        checked_at=r.checked_at,
    )


def to_payment(r: PaymentRow, attempts: list[AttemptRow]) -> Payment:
    return Payment(
        id=r.id,
        user_id=r.user_id,
        price=Money(amount=clean(r.price_amount), currency=r.price_currency),
        pay_currency=r.pay_currency,
        purpose=Purpose(domain=r.purpose_domain, ref=r.purpose_ref),
        method=PaymentMethod(r.method),
        status=PaymentStatus(r.status),
        attempts=[to_attempt(a) for a in sorted(attempts, key=lambda a: a.seq)],
        received_total=clean(r.received_total),
        round=r.round,
        idempotency_key=r.idempotency_key,
        request_hash=r.request_hash,
        failure_reason=r.failure_reason,
        version=r.version,
        created_at=r.created_at,
        updated_at=r.updated_at,
    )
