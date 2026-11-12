import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text

from modules.payments.application.errors import ConcurrencyConflict, DuplicatePayment
from modules.payments.application.ports import ProviderEventRef
from modules.payments.domain.entities import Payment
from modules.payments.domain.errors import AttemptFailure, FailureClass
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.statuses import PaymentStatus as P
from modules.payments.domain.value_objects import (
    Checkout,
    Money,
    PaymentMethod,
    ProviderCode,
    Purpose,
)

pytestmark = pytest.mark.integration
NOW = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)
NP = ProviderCode("nowpayments")


def new_payment(key="k1", user=None) -> Payment:
    return Payment.create(
        user_id=user or uuid4(),
        price=Money(amount="9.99", currency="USD"),
        pay_currency="usdttrc20",
        purpose=Purpose(domain="x", ref="1"),
        idempotency_key=key,
        request_hash="h",
        now=NOW,
    )


async def persisted_awaiting(uow_factory) -> Payment:
    p = new_payment()
    async with uow_factory() as uow:
        await uow.payments.add(p)
        await uow.commit()
    async with uow_factory() as uow:
        p = await uow.payments.get_for_update(p.id)
        a1 = p.start_attempt(NP, NOW)
        p.apply_create_failure(
            a1.id, AttemptFailure(cls=FailureClass.TIMEOUT_UNKNOWN, code="t"), NOW
        )
        a2 = p.start_attempt(ProviderCode("coingate"), NOW)
        p.apply_create_success(
            a2.id,
            "cg-1",
            Checkout(
                invoice_url="http://pay", pay_amount="1.5", expires_at=NOW + timedelta(minutes=30)
            ),
            NOW,
        )
        await uow.payments.save(p)
        await uow.commit()
    return p


async def test_roundtrip_payment_with_attempts(uow_factory):
    p = await persisted_awaiting(uow_factory)
    async with uow_factory() as uow:
        got = await uow.payments.get(p.id)
    assert got is not None
    assert got.status is P.AWAITING_CUSTOMER
    assert got.price == Money(amount="9.99", currency="usd")
    assert [(a.provider, a.status) for a in got.attempts] == [
        ("nowpayments", A.SUPERSEDED),
        ("coingate", A.CREATED),
    ]
    assert got.attempts[0].failure.cls is FailureClass.TIMEOUT_UNKNOWN
    assert got.attempts[1].checkout.invoice_url == "http://pay"
    assert got.attempts[1].checkout.expires_at == NOW + timedelta(minutes=30)
    assert got.version == 1


async def test_duplicate_idempotency_raises(uow_factory):
    user = uuid4()
    async with uow_factory() as uow:
        await uow.payments.add(new_payment("same", user))
        await uow.commit()
    with pytest.raises(DuplicatePayment):
        async with uow_factory() as uow:
            await uow.payments.add(new_payment("same", user))
            await uow.commit()
    async with uow_factory() as uow:
        assert (await uow.payments.find_by_idempotency(user, "same")) is not None


async def test_events_written_to_outbox_on_commit(uow_factory):
    p = new_payment()
    async with uow_factory() as uow:
        await uow.payments.add(p)
        await uow.commit()
    async with uow_factory() as uow:
        p = await uow.payments.get_for_update(p.id)
        p.fail_no_provider(NOW)
        await uow.payments.save(p)
        await uow.commit()
    async with uow_factory() as uow:
        recs = await uow.outbox.fetch_unpublished(10)
        assert [r.event_type for r in recs] == ["PaymentFailed"]
        assert recs[0].payload["reason"] == "no_provider_available"
        await uow.outbox.mark_published([recs[0].id])
        await uow.commit()
    async with uow_factory() as uow:
        assert await uow.outbox.fetch_unpublished(10) == []


async def test_rollback_without_commit(uow_factory):
    p = new_payment()
    async with uow_factory() as uow:
        await uow.payments.add(p)
    async with uow_factory() as uow:
        assert await uow.payments.get(p.id) is None


async def test_stale_version_conflict(uow_factory):
    p = new_payment()
    async with uow_factory() as uow:
        await uow.payments.add(p)
        await uow.commit()
    async with uow_factory() as uow:
        stale = await uow.payments.get(p.id)
    async with uow_factory() as uow:
        fresh = await uow.payments.get_for_update(p.id)
        fresh.fail_no_provider(NOW)
        await uow.payments.save(fresh)
        await uow.commit()
    with pytest.raises(ConcurrencyConflict):
        async with uow_factory() as uow:
            stale.fail_no_provider(NOW)
            await uow.payments.save(stale)
            await uow.commit()


async def test_inbox_dedup(uow_factory):
    ref = ProviderEventRef(provider_payment_id="1", order_id=uuid4())
    async with uow_factory() as uow:
        e1 = await uow.events.add(NP, "hash", ref, "body")
        e2 = await uow.events.add(NP, "hash", ref, "body")
        await uow.commit()
    assert e1 is not None and e2 is None
    async with uow_factory() as uow:
        ev = await uow.events.get(e1)
        assert (
            ev.provider_payment_id == "1"
            and ev.order_id == ref.order_id
            and ev.processed_at is None
        )
        await uow.events.mark_processed(e1, note="ok")
        await uow.commit()
    async with uow_factory() as uow:
        assert (await uow.events.get(e1)).processed_at is not None


async def test_concurrent_apply_single_event(uow_factory):
    p = await persisted_awaiting(uow_factory)
    attempt_id = p.attempts[1].id

    async def apply():
        async with uow_factory() as uow:
            pay = await uow.payments.get_for_update(p.id)
            await asyncio.sleep(0.05)
            pay.apply_provider_state(
                attempt_id, A.FINISHED, Money(amount="1.5", currency="usdttrc20"), NOW
            )
            await uow.payments.save(pay)
            await uow.commit()

    await asyncio.gather(apply(), apply())
    async with uow_factory() as uow:
        recs = await uow.outbox.fetch_unpublished(10)
        got = await uow.payments.get(p.id)
    assert [r.event_type for r in recs] == ["PaymentSucceeded"]
    assert (
        got.status is P.SUCCEEDED and got.received_total == Money(amount="1.5", currency="x").amount
    )


async def test_find_attempt(uow_factory):
    p = await persisted_awaiting(uow_factory)
    async with uow_factory() as uow:
        assert await uow.payments.find_attempt(
            ProviderCode("coingate"), provider_payment_id="cg-1"
        ) == (
            p.id,
            p.attempts[1].id,
        )
        assert await uow.payments.find_attempt(NP, attempt_id=p.attempts[0].id) == (
            p.id,
            p.attempts[0].id,
        )
        assert await uow.payments.find_attempt(NP, provider_payment_id="cg-1") is None


async def test_attempts_to_reconcile_filters(uow_factory):
    p = await persisted_awaiting(uow_factory)
    async with uow_factory() as uow:
        soon = await uow.payments.attempts_to_reconcile(NOW + timedelta(seconds=30), 120, 900, 100)
        later = await uow.payments.attempts_to_reconcile(NOW + timedelta(minutes=5), 120, 900, 100)
        too_old = await uow.payments.attempts_to_reconcile(NOW + timedelta(days=3), 120, 900, 100)
    assert soon == []
    # попытка без provider_payment_id (timeout) не сверяется — её id у провайдера неизвестен
    assert later == [(p.id, p.attempts[1].id)]
    assert too_old == []


async def test_attempts_past_expiry(uow_factory):
    p = await persisted_awaiting(uow_factory)
    async with uow_factory() as uow:
        assert await uow.payments.attempts_past_expiry(NOW + timedelta(minutes=35), 10, 100) == []
        assert await uow.payments.attempts_past_expiry(NOW + timedelta(minutes=41), 10, 100) == [
            (p.id, p.attempts[1].id)
        ]


async def test_route_wildcard(uow_factory, sessionmaker):
    async with sessionmaker() as s:
        await s.execute(
            text(
                "INSERT INTO cascade_routes(method, pay_currency, providers, strategy, enabled)"
                " VALUES ('CRYPTO','btc', ARRAY['coingate'], 'priority', true)"
            )
        )
        await s.commit()
    async with uow_factory() as uow:
        assert await uow.routes.get(PaymentMethod.CRYPTO, "btc") == ["coingate"]
        assert await uow.routes.get(PaymentMethod.CRYPTO, "eth") == ["nowpayments", "coingate"]
        assert await uow.routes.currencies() == ["btc"]


def test_clean_decimal():
    from decimal import Decimal

    from modules.payments.infrastructure.db.mappers import clean

    assert str(clean(Decimal("9.990000000000000000"))) == "9.99"
    assert str(clean(Decimal("10.000000000000000000"))) == "10"
    assert str(clean(Decimal("0E-18"))) == "0"


async def test_reconcile_closed_attempts_backoff_and_active_first(uow_factory):
    p = await persisted_awaiting(uow_factory)
    async with uow_factory() as uow:
        pay = await uow.payments.get_for_update(p.id)
        pay.attempts[0].provider_payment_id = "np-ghost"  # закрытая попытка с известным id
        await uow.payments.save(pay)
        await uow.commit()
    async with uow_factory() as uow:
        at5 = await uow.payments.attempts_to_reconcile(NOW + timedelta(minutes=5), 120, 900, 100)
        at20 = await uow.payments.attempts_to_reconcile(NOW + timedelta(minutes=20), 120, 900, 100)
    assert at5 == [(p.id, p.attempts[1].id)]
    assert at20 == [(p.id, p.attempts[1].id), (p.id, p.attempts[0].id)]  # активная первой


async def test_stuck_payments(uow_factory):
    p = new_payment()
    async with uow_factory() as uow:
        await uow.payments.add(p)
        await uow.commit()
    done = await persisted_awaiting(uow_factory)
    async with uow_factory() as uow:
        assert await uow.payments.stuck_payments(NOW + timedelta(seconds=30), 60, 10) == []
        assert await uow.payments.stuck_payments(NOW + timedelta(minutes=2), 60, 10) == [p.id]
    assert done.id != p.id


async def test_inbox_unprocessed(uow_factory, sessionmaker):
    ref = ProviderEventRef(provider_payment_id="1", order_id=None)
    async with uow_factory() as uow:
        e1 = await uow.events.add(NP, "h1", ref, "")
        e2 = await uow.events.add(NP, "h2", ref, "")
        await uow.events.mark_processed(e2)
        await uow.commit()
    now = datetime.now(UTC)
    async with uow_factory() as uow:
        assert await uow.events.unprocessed(now, 30, 10) == []
        assert await uow.events.unprocessed(now + timedelta(minutes=1), 30, 10) == [e1]


async def test_duplicate_provider_payment_id_raises(uow_factory):
    from modules.payments.application.errors import DuplicateProviderPaymentId

    await persisted_awaiting(uow_factory)  # coingate / cg-1
    p = new_payment("k2")
    async with uow_factory() as uow:
        await uow.payments.add(p)
        await uow.commit()
    with pytest.raises(DuplicateProviderPaymentId):
        async with uow_factory() as uow:
            pay = await uow.payments.get_for_update(p.id)
            a = pay.start_attempt(ProviderCode("coingate"), NOW)
            pay.apply_create_success(
                a.id, "cg-1", Checkout(invoice_url="x", expires_at=NOW + timedelta(minutes=30)), NOW
            )
            await uow.payments.save(pay)
            await uow.commit()
