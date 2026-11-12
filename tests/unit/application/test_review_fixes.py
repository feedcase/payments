"""Регрессии по итогам финального ревью (C1, I3, I4, duplicate provider id)."""

import asyncio
from uuid import uuid4

from modules.payments.application.ports import ProviderEventRef, ProviderPaymentState
from modules.payments.application.use_cases import (
    ApplyProviderState,
    CreatePayment,
    CreatePaymentCommand,
    ProcessProviderEvent,
    Reconcile,
    RecoverStuck,
)
from modules.payments.domain.errors import FailureClass
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.statuses import PaymentStatus as P
from modules.payments.domain.value_objects import Money, ProviderCode, Purpose
from tests.fakes import FakeDispatcher, FakeLock, FakeProvider, FakeRateLimiter


def wire(env):
    dispatcher = FakeDispatcher()
    apply = ApplyProviderState(
        uow_factory=env.uow,
        registry=env.registry,
        cascade=env.cascade,
        dispatcher=dispatcher,
        clock=env.clock,
    )
    process = ProcessProviderEvent(uow_factory=env.uow, apply=apply)
    recover = RecoverStuck(
        uow_factory=env.uow,
        cascade=env.cascade,
        process_event=process,
        clock=env.clock,
        settings=env.settings,
    )
    reconcile = Reconcile(uow_factory=env.uow, apply=apply, clock=env.clock, settings=env.settings)
    return recover, reconcile, process


async def stuck_creating(env):
    """Процесс упал после tx1: попытка CREATING сохранена, исход не применён."""
    p = await env.new_payment()
    async with env.uow() as uow:
        pay = await uow.payments.get_for_update(p.id)
        pay.start_attempt(ProviderCode("p1"), env.clock.now())
        await uow.payments.save(pay)
        await uow.commit()
    return p.id


async def test_recover_orphaned_creating_continues_cascade(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    pid = await stuck_creating(env)
    recover, _, _ = wire(env)
    env.clock.advance(120)
    assert await recover() >= 1
    got = env.payment(pid)
    assert [(a.provider, a.status) for a in got.attempts] == [
        ("p1", A.SUPERSEDED),
        ("p2", A.CREATED),
    ]
    assert got.attempts[0].failure.code == "orphaned"
    assert got.status is P.AWAITING_CUSTOMER


async def test_recover_resumes_routing_without_attempt(make_env):
    env = make_env(FakeProvider("p1"))
    p = await env.new_payment()  # NEW, каскад так и не запустился (сбой постановки в очередь)
    recover, _, _ = wire(env)
    env.clock.advance(120)
    await recover()
    assert env.payment(p.id).status is P.AWAITING_CUSTOMER


async def test_recover_skips_fresh(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    pid = await stuck_creating(env)
    recover, _, _ = wire(env)
    env.clock.advance(10)
    await recover()
    assert env.payment(pid).attempts[0].status is A.CREATING


async def test_recover_reprocesses_unprocessed_inbox(make_env):
    p1 = FakeProvider("p1")
    env = make_env(p1)
    p = await env.new_payment()
    await env.cascade.run(p.id)
    a = env.payment(p.id).attempts[0]
    p1.states[a.provider_payment_id] = ProviderPaymentState(
        status=A.FINISHED, received=Money(amount="9.99", currency="usdttrc20")
    )
    async with env.uow() as uow:
        eid = await uow.events.add(
            "p1",
            "h",
            ProviderEventRef(provider_payment_id=a.provider_payment_id, order_id=None),
            "",
        )
        await uow.commit()
    recover, _, _ = wire(env)
    env.clock.advance(120)
    await recover()
    assert env.store.inbox[eid]["processed_at"] is not None
    assert env.payment(p.id).status is P.SUCCEEDED


async def test_duplicate_provider_id_goes_next(make_env):
    p1 = FakeProvider("p1")
    p1.fixed_pid = "dup"
    env = make_env(p1, FakeProvider("p2"))
    first = await env.new_payment()
    await env.cascade.run(first.id)
    second = await env.new_payment()
    assert await env.cascade.run(second.id) == "completed"
    got = env.payment(second.id)
    assert got.attempts[0].failure.cls is FailureClass.PROVIDER_ERROR
    assert got.attempts[0].failure.code == "duplicate_provider_id"
    assert got.attempts[1].provider == "p2" and got.status is P.AWAITING_CUSTOMER


async def test_create_survives_dispatcher_failure(make_env):
    p1 = FakeProvider("p1")
    env = make_env(p1, FakeProvider("p2"))

    class Broken(FakeDispatcher):
        async def continue_later(self, payment_id):
            raise ConnectionError("broker down")

    async def slow(cmd):
        env.clock.advance(13)
        raise ConnectionError("boom")

    p1.create = slow
    create = CreatePayment(
        uow_factory=env.uow,
        cascade=env.cascade,
        dispatcher=Broken(),
        lock=FakeLock(),
        rate_limiter=FakeRateLimiter(),
        clock=env.clock,
    )
    r = await create(
        CreatePaymentCommand(
            user_id=uuid4(),
            price=Money(amount="9.99", currency="USD"),
            pay_currency="usdttrc20",
            purpose=Purpose(domain="x", ref="1"),
            idempotency_key="k",
        )
    )
    assert r.deferred and r.view.status is P.ROUTING  # sweeper продолжит


async def test_reconcile_backs_off_closed_attempts(make_env):
    p1 = FakeProvider("p1")
    env = make_env(p1)
    p = await env.new_payment()
    await env.cascade.run(p.id)
    pay = env.payment(p.id)
    pay.attempts[0].status = A.SUPERSEDED  # закрытая попытка: опрос реже
    env.store.payments[p.id] = pay
    _, reconcile, _ = wire(env)
    env.clock.advance(300)
    assert await reconcile() == 0
    env.clock.advance(env.settings.reconcile_closed_after_s)
    assert await reconcile() == 1


async def test_reconcile_fetches_concurrently(make_env):
    p1 = FakeProvider("p1")
    env = make_env(p1)
    for _ in range(3):
        p = await env.new_payment()
        await env.cascade.run(p.id)
    in_flight, peak, gate = 0, 0, asyncio.Event()

    async def fetch(pid):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        if peak >= 3:
            gate.set()
        await asyncio.wait_for(gate.wait(), 1)
        in_flight -= 1
        return ProviderPaymentState(status=A.CONFIRMING)

    p1.fetch = fetch
    _, reconcile, _ = wire(env)
    env.clock.advance(300)
    assert await reconcile() == 3
    assert peak == 3
