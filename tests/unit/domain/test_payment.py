from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from modules.payments.domain.entities import Payment
from modules.payments.domain.errors import AttemptFailure, DomainError, FailureClass
from modules.payments.domain.events import (
    PaymentExpired,
    PaymentFailed,
    PaymentOverpaid,
    PaymentPartiallyPaid,
    PaymentSucceeded,
)
from modules.payments.domain.policies import Decision
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.statuses import PaymentStatus as P
from modules.payments.domain.value_objects import Checkout, Money, ProviderCode, Purpose

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)
NP = ProviderCode("nowpayments")
CG = ProviderCode("coingate")


def make() -> Payment:
    return Payment.create(
        user_id=uuid4(),
        price=Money(amount="9.99", currency="USD"),
        pay_currency="usdttrc20",
        purpose=Purpose(domain="x", ref="1"),
        idempotency_key="k1",
        request_hash="h",
        now=NOW,
    )


def checkout() -> Checkout:
    return Checkout(pay_address="addr", expires_at=NOW + timedelta(minutes=30))


def fail(cls: FailureClass) -> AttemptFailure:
    return AttemptFailure(cls=cls, code="c")


def awaiting(p: Payment, provider=NP):
    a = p.start_attempt(provider, NOW)
    p.apply_create_success(a.id, f"pp-{a.seq}", checkout(), NOW)
    return a


def usdt(x: str) -> Money:
    return Money(amount=x, currency="usdttrc20")


def test_start_attempt_sets_routing_and_seq():
    p = make()
    a1 = p.start_attempt(NP, NOW)
    assert p.status is P.ROUTING and a1.seq == 1 and a1.status is A.CREATING
    p.apply_create_failure(a1.id, fail(FailureClass.PROVIDER_ERROR), NOW)
    a2 = p.start_attempt(CG, NOW)
    assert a2.seq == 2


def test_cannot_start_with_active_attempt():
    p = make()
    p.start_attempt(NP, NOW)
    with pytest.raises(DomainError):
        p.start_attempt(CG, NOW)


def test_create_success_awaiting_customer():
    p = make()
    a = p.start_attempt(NP, NOW)
    assert p.apply_create_success(a.id, "pp1", checkout(), NOW) is Decision.DONE
    assert p.status is P.AWAITING_CUSTOMER
    assert a.status is A.CREATED and a.provider_payment_id == "pp1"


def test_timeout_unknown_goes_next_and_superseded_on_next_start():
    p = make()
    a1 = p.start_attempt(NP, NOW)
    d = p.apply_create_failure(a1.id, fail(FailureClass.TIMEOUT_UNKNOWN), NOW)
    assert d is Decision.NEXT_PROVIDER and a1.status is A.CREATE_UNKNOWN
    p.start_attempt(CG, NOW)
    assert a1.status is A.SUPERSEDED


def test_invalid_request_stops_and_fails():
    p = make()
    a = p.start_attempt(NP, NOW)
    d = p.apply_create_failure(
        a.id, AttemptFailure(cls=FailureClass.INVALID_REQUEST, code="http_401"), NOW
    )
    assert d is Decision.STOP and p.status is P.FAILED
    (ev,) = p.pull_events()
    assert isinstance(ev, PaymentFailed) and ev.reason == "INVALID_REQUEST:http_401"


def test_unsupported_goes_next():
    p = make()
    a = p.start_attempt(NP, NOW)
    assert (
        p.apply_create_failure(a.id, fail(FailureClass.UNSUPPORTED), NOW) is Decision.NEXT_PROVIDER
    )
    assert p.status is P.ROUTING and NP in p.tried_providers()


def test_fail_no_provider():
    p = make()
    p.fail_no_provider(NOW)
    assert p.status is P.FAILED and p.failure_reason == "no_provider_available"
    assert isinstance(p.pull_events()[0], PaymentFailed)


def test_finished_succeeds_once():
    p = make()
    a = awaiting(p)
    assert p.apply_provider_state(a.id, A.FINISHED, usdt("9.99"), NOW) is Decision.DONE
    assert p.status is P.SUCCEEDED
    assert [type(e) for e in p.pull_events()] == [PaymentSucceeded]
    assert p.apply_provider_state(a.id, A.FINISHED, usdt("9.99"), NOW) is None
    assert p.pull_events() == []


def test_stale_state_ignored():
    p = make()
    a = awaiting(p)
    p.apply_provider_state(a.id, A.FINISHED, usdt("9.99"), NOW)
    assert p.apply_provider_state(a.id, A.CONFIRMING, None, NOW) is None
    assert a.status is A.FINISHED and p.status is P.SUCCEEDED


def test_confirming_moves_payment():
    p = make()
    a = awaiting(p)
    p.apply_provider_state(a.id, A.CONFIRMING, None, NOW)
    assert p.status is P.CONFIRMING


def test_expired_emits_event():
    p = make()
    a = awaiting(p)
    p.apply_provider_state(a.id, A.EXPIRED, None, NOW)
    assert p.status is P.EXPIRED
    assert isinstance(p.pull_events()[0], PaymentExpired)


def test_customer_failed_returns_next_provider_and_excludes_provider():
    p = make()
    a = awaiting(p)
    assert p.apply_provider_state(a.id, A.FAILED, None, NOW) is Decision.NEXT_PROVIDER
    assert p.status is P.ROUTING
    assert a.failure is not None and a.failure.cls is FailureClass.CUSTOMER_FAILED
    assert NP in p.tried_providers()


def test_late_payment_on_superseded_before_success():
    p = make()
    a1 = p.start_attempt(NP, NOW)
    p.apply_create_failure(a1.id, fail(FailureClass.TIMEOUT_UNKNOWN), NOW)
    a2 = awaiting(p, CG)
    assert a1.status is A.SUPERSEDED
    p.apply_provider_state(a1.id, A.FINISHED, usdt("9.99"), NOW)
    assert p.status is P.SUCCEEDED
    assert a2.status is A.SUPERSEDED
    ev = p.pull_events()
    assert [type(e) for e in ev] == [PaymentSucceeded] and ev[0].attempt_id == a1.id


def test_late_payment_on_expired_after_success_overpaid():
    p = make()
    a1 = awaiting(p, NP)
    p.apply_provider_state(a1.id, A.EXPIRED, None, NOW)
    p.reinvoice(NOW)
    a2 = awaiting(p, CG)
    p.apply_provider_state(a2.id, A.FINISHED, usdt("9.99"), NOW)
    p.pull_events()
    p.apply_provider_state(a1.id, A.FINISHED, usdt("9.99"), NOW)
    assert p.status is P.SUCCEEDED
    (ev,) = p.pull_events()
    assert isinstance(ev, PaymentOverpaid) and ev.attempt_id == a1.id
    assert p.received_total == usdt("19.98").amount


def test_partial_then_finished_received_total_not_doubled():
    p = make()
    a = awaiting(p)
    p.apply_provider_state(a.id, A.PARTIALLY_PAID, usdt("5"), NOW)
    assert p.status is P.PARTIALLY_PAID
    assert isinstance(p.pull_events()[0], PaymentPartiallyPaid)
    p.apply_provider_state(a.id, A.FINISHED, usdt("9.99"), NOW)
    assert p.received_total == usdt("9.99").amount


def test_reinvoice_after_expired_new_round_allows_same_provider():
    p = make()
    a = awaiting(p)
    p.apply_provider_state(a.id, A.EXPIRED, None, NOW)
    p.reinvoice(NOW)
    assert p.status is P.ROUTING and p.round == 2
    assert p.tried_providers() == set()


def test_reinvoice_after_no_provider():
    p = make()
    p.fail_no_provider(NOW)
    p.reinvoice(NOW)
    assert p.status is P.ROUTING and p.failure_reason is None


def test_reinvoice_rejected_when_succeeded():
    p = make()
    a = awaiting(p)
    p.apply_provider_state(a.id, A.FINISHED, usdt("9.99"), NOW)
    with pytest.raises(DomainError):
        p.reinvoice(NOW)


def test_reinvoice_rejected_after_invalid_request():
    p = make()
    a = p.start_attempt(NP, NOW)
    p.apply_create_failure(a.id, fail(FailureClass.INVALID_REQUEST), NOW)
    with pytest.raises(DomainError):
        p.reinvoice(NOW)


def test_mark_checked_sets_checked_at():
    p = make()
    a = awaiting(p)
    later = NOW + timedelta(minutes=5)
    p.mark_checked(a.id, later)
    assert a.checked_at == later


def expired_then_reinvoiced(p: Payment):
    a1 = awaiting(p, NP)
    p.apply_provider_state(a1.id, A.EXPIRED, None, NOW)
    p.reinvoice(NOW)
    return a1


def test_create_outcome_after_late_success_records_ghost():
    """Review I1: опоздавшая оплата старого счёта, пока новый счёт ещё создаётся."""
    p = make()
    a1 = expired_then_reinvoiced(p)
    a2 = p.start_attempt(CG, NOW)
    p.apply_provider_state(a1.id, A.FINISHED, usdt("9.99"), NOW)
    assert p.status is P.SUCCEEDED and a2.status is A.SUPERSEDED
    assert p.apply_create_success(a2.id, "cg-9", checkout(), NOW) is Decision.DONE
    assert a2.provider_payment_id == "cg-9" and a2.status is A.SUPERSEDED
    assert p.status is P.SUCCEEDED


def test_create_failure_after_late_success_is_noop():
    p = make()
    a1 = expired_then_reinvoiced(p)
    a2 = p.start_attempt(CG, NOW)
    p.apply_provider_state(a1.id, A.FINISHED, usdt("9.99"), NOW)
    p.pull_events()
    assert p.apply_create_failure(a2.id, fail(FailureClass.INVALID_REQUEST), NOW) is Decision.DONE
    assert p.status is P.SUCCEEDED and p.pull_events() == []


def test_superseding_partially_paid_keeps_money():
    """Review I2: деньги по частично оплаченной попытке не пропадают при замещении."""
    p = make()
    a1 = expired_then_reinvoiced(p)
    a2 = awaiting(p, CG)
    p.apply_provider_state(a2.id, A.PARTIALLY_PAID, usdt("0.4"), NOW)
    p.pull_events()
    p.apply_provider_state(a1.id, A.FINISHED, usdt("1"), NOW)
    events = p.pull_events()
    assert [type(e) for e in events] == [PaymentSucceeded, PaymentOverpaid]
    assert events[1].attempt_id == a2.id and events[1].received == usdt("0.4")
    assert p.received_total == usdt("1.4").amount
    p.apply_provider_state(a1.id, A.CONFIRMING, None, NOW)  # no-op, пересчёт не теряет 0.4
    assert p.received_total == usdt("1.4").amount
    p.apply_provider_state(a2.id, A.FINISHED, usdt("1"), NOW)  # покупатель доплатил по a2
    assert p.received_total == usdt("2").amount


def test_orphan_creating_attempt():
    """Review C1: попытка, зависшая в CREATING (процесс упал между транзакциями)."""
    p = make()
    a = p.start_attempt(NP, NOW)
    p.orphan_attempt(a.id, NOW)
    assert a.status is A.CREATE_UNKNOWN
    assert a.failure.cls is FailureClass.TIMEOUT_UNKNOWN and a.failure.code == "orphaned"
    assert p.status is P.ROUTING and NP in p.tried_providers()
    p.start_attempt(CG, NOW)
    assert a.status is A.SUPERSEDED


def test_orphan_ignores_non_creating():
    p = make()
    a = awaiting(p)
    p.orphan_attempt(a.id, NOW)
    assert a.status is A.CREATED
