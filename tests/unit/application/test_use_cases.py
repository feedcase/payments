import asyncio
from uuid import uuid4

import pytest

from modules.payments.application.errors import (
    IdempotencyMismatch,
    InvalidState,
    NotFound,
    RateLimited,
)
from modules.payments.application.ports import ProviderEventRef, ProviderPaymentState
from modules.payments.application.use_cases import (
    ApplyProviderState,
    CreatePayment,
    CreatePaymentCommand,
    ExpireStale,
    GetPayment,
    ProcessProviderEvent,
    Reconcile,
    RefreshCapabilities,
    Reinvoice,
    RelayOutbox,
)
from modules.payments.domain.errors import FailureClass
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.statuses import PaymentStatus as P
from modules.payments.domain.value_objects import Money, Purpose
from tests.fakes import (
    FakeDispatcher,
    FakeLock,
    FakeProvider,
    FakePublisher,
    FakeRateLimiter,
    failure,
)

USER = uuid4()


def cmd(key="k1", amount="9.99", user=USER):
    return CreatePaymentCommand(
        user_id=user,
        price=Money(amount=amount, currency="USD"),
        pay_currency="usdttrc20",
        purpose=Purpose(domain="x", ref="1"),
        idempotency_key=key,
    )


class UC:
    def __init__(self, env, lock=None, limiter=None):
        self.env = env
        self.dispatcher = FakeDispatcher()
        self.lock = lock or FakeLock()
        self.create = CreatePayment(
            uow_factory=env.uow,
            cascade=env.cascade,
            dispatcher=self.dispatcher,
            lock=self.lock,
            rate_limiter=limiter or FakeRateLimiter(),
            clock=env.clock,
        )
        self.get = GetPayment(uow_factory=env.uow)
        self.reinvoice = Reinvoice(
            uow_factory=env.uow, cascade=env.cascade, dispatcher=self.dispatcher, clock=env.clock
        )
        self.apply = ApplyProviderState(
            uow_factory=env.uow,
            registry=env.registry,
            cascade=env.cascade,
            dispatcher=self.dispatcher,
            clock=env.clock,
        )
        self.process = ProcessProviderEvent(uow_factory=env.uow, apply=self.apply)
        self.reconcile = Reconcile(
            uow_factory=env.uow, apply=self.apply, clock=env.clock, settings=env.settings
        )
        self.expire = ExpireStale(
            uow_factory=env.uow,
            registry=env.registry,
            apply=self.apply,
            clock=env.clock,
            settings=env.settings,
        )


@pytest.fixture
def uc(make_env):
    def build(*providers, **kw):
        return UC(make_env(*providers), **kw)

    return build


async def test_create_payment_happy(uc):
    u = uc(FakeProvider("p1"))
    r = await u.create(cmd())
    assert r.created and not r.deferred
    assert r.view.status is P.AWAITING_CUSTOMER
    assert r.view.attempt.provider == "p1" and r.view.attempt.checkout.pay_address
    assert u.lock.held == set()


async def test_create_same_key_same_body_returns_existing(uc):
    u = uc(FakeProvider("p1"))
    r1 = await u.create(cmd())
    r2 = await u.create(cmd())
    assert not r2.created and r2.view.payment_id == r1.view.payment_id
    assert len(u.env.store.payments) == 1


async def test_create_same_key_other_body_mismatch(uc):
    u = uc(FakeProvider("p1"))
    await u.create(cmd())
    with pytest.raises(IdempotencyMismatch):
        await u.create(cmd(amount="10.00"))


async def test_create_payment_without_lock(uc):
    u = uc(FakeProvider("p1"), lock=FakeLock(broken=True))
    r = await u.create(cmd())
    assert r.created and r.view.status is P.AWAITING_CUSTOMER


async def test_create_concurrent_same_key_one_payment(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    results = await asyncio.gather(*(u.create(cmd()) for _ in range(5)))
    assert len(u.env.store.payments) == 1
    assert sum(r.created for r in results) == 1
    assert len(p1.created) == 1


async def test_create_deferred_dispatches(uc):
    p1 = FakeProvider("p1")
    u = uc(p1, FakeProvider("p2"))

    async def slow(c):
        u.env.clock.advance(13)
        raise failure(FailureClass.PROVIDER_ERROR)

    p1.create = slow
    r = await u.create(cmd())
    assert r.deferred and u.dispatcher.deferred == [r.view.payment_id]
    assert r.view.status is P.ROUTING


async def test_rate_limited(uc):
    u = uc(FakeProvider("p1"), limiter=FakeRateLimiter(allow=False))
    with pytest.raises(RateLimited):
        await u.create(cmd())


async def test_rate_limit_does_not_block_replay(uc):
    limiter = FakeRateLimiter()
    u = uc(FakeProvider("p1"), limiter=limiter)
    await u.create(cmd())
    limiter._allow = False
    r = await u.create(cmd())
    assert not r.created


async def test_get_foreign_payment_not_found(uc):
    u = uc(FakeProvider("p1"))
    r = await u.create(cmd())
    with pytest.raises(NotFound):
        await u.get(uuid4(), r.view.payment_id)
    assert (await u.get(USER, r.view.payment_id)).payment_id == r.view.payment_id


async def test_apply_state_customer_failed_runs_next_provider(uc):
    p1, p2 = FakeProvider("p1"), FakeProvider("p2")
    u = uc(p1, p2)
    r = await u.create(cmd())
    pay = u.env.payment(r.view.payment_id)
    a = pay.attempts[0]
    p1.states[a.provider_payment_id] = ProviderPaymentState(status=A.FAILED)
    await u.apply(pay.id, a.id)
    got = u.env.payment(pay.id)
    assert [x.provider for x in got.attempts] == ["p1", "p2"]
    assert got.status is P.AWAITING_CUSTOMER


async def test_apply_state_finished_emits_succeeded(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    r = await u.create(cmd())
    pay = u.env.payment(r.view.payment_id)
    a = pay.attempts[0]
    p1.states[a.provider_payment_id] = ProviderPaymentState(
        status=A.FINISHED, received=Money(amount="9.99", currency="usdttrc20")
    )
    await u.apply(pay.id, a.id)
    assert u.env.payment(pay.id).status is P.SUCCEEDED
    assert u.env.uow().events_of(pay.id) == ["PaymentSucceeded"]
    assert u.env.payment(pay.id).attempts[0].checked_at == u.env.clock.now()


async def _inbox_event(u, provider, pid):
    async with u.env.uow() as uow:
        eid = await uow.events.add(
            provider, f"h-{pid}", ProviderEventRef(provider_payment_id=pid, order_id=None), "{}"
        )
        await uow.commit()
    return eid


async def test_process_event_idempotent(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    r = await u.create(cmd())
    a = u.env.payment(r.view.payment_id).attempts[0]
    eid = await _inbox_event(u, "p1", a.provider_payment_id)
    await u.process(eid)
    await u.process(eid)
    assert p1.fetch_calls == 1


async def test_process_event_unknown_attempt(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    eid = await _inbox_event(u, "p1", "nope")
    await u.process(eid)
    assert u.env.store.inbox[eid]["note"] == "unknown_attempt"
    assert p1.fetch_calls == 0


async def test_reconcile_picks_superseded_and_finishes_late(uc):
    p1 = FakeProvider("p1", [failure(FailureClass.TIMEOUT_UNKNOWN)])
    p2 = FakeProvider("p2")
    u = uc(p1, p2)
    r = await u.create(cmd())
    pay = u.env.payment(r.view.payment_id)
    # ghost: провайдер всё же создал счёт и знает его id (как после ответа по order_id)
    pay.attempts[0].provider_payment_id = "ghost"
    u.env.store.payments[pay.id] = pay
    p1.states["ghost"] = ProviderPaymentState(
        status=A.FINISHED, received=Money(amount="9.99", currency="usdttrc20")
    )
    u.env.clock.advance(u.env.settings.reconcile_closed_after_s + 1)
    n = await u.reconcile()
    assert n >= 1
    got = u.env.payment(pay.id)
    assert got.status is P.SUCCEEDED and got.attempts[0].status is A.FINISHED
    assert got.attempts[1].status is A.SUPERSEDED


async def test_reconcile_continues_after_one_error(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    r1 = await u.create(cmd("a"))
    r2 = await u.create(cmd("b"))
    calls = []

    async def fetch(pid):
        calls.append(pid)
        if len(calls) == 1:
            raise failure(FailureClass.PROVIDER_UNAVAILABLE)
        return ProviderPaymentState(status=A.CONFIRMING)

    p1.fetch = fetch
    u.env.clock.advance(300)
    await u.reconcile()
    assert len(calls) == 2
    statuses = {u.env.payment(r.view.payment_id).status for r in (r1, r2)}
    assert P.CONFIRMING in statuses


async def test_expire_stale_marks_expired(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    r = await u.create(cmd())
    u.env.clock.advance(60 * 60)
    assert await u.expire() == 1
    assert u.env.payment(r.view.payment_id).status is P.EXPIRED


async def test_expire_stale_keeps_confirming(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    r = await u.create(cmd())
    pid = u.env.payment(r.view.payment_id).attempts[0].provider_payment_id
    p1.states[pid] = ProviderPaymentState(status=A.CONFIRMING)
    u.env.clock.advance(60 * 60)
    await u.expire()
    assert u.env.payment(r.view.payment_id).status is P.CONFIRMING


async def test_reinvoice_after_expiry(uc):
    p1 = FakeProvider("p1")
    u = uc(p1)
    r = await u.create(cmd())
    u.env.clock.advance(60 * 60)
    await u.expire()
    r2 = await u.reinvoice(USER, r.view.payment_id)
    assert r2.view.status is P.AWAITING_CUSTOMER and r2.view.attempt.seq == 2


async def test_reinvoice_invalid_state(uc):
    u = uc(FakeProvider("p1"))
    r = await u.create(cmd())
    with pytest.raises(InvalidState):
        await u.reinvoice(USER, r.view.payment_id)


async def test_refresh_capabilities_survives_one_failure(uc):
    bad = FakeProvider("p1")

    async def boom(c):
        raise RuntimeError("down")

    bad.capabilities = boom
    u = uc(bad, FakeProvider("p2"))
    refresh = RefreshCapabilities(uow_factory=u.env.uow, registry=u.env.registry, caps=u.env.caps)
    assert await refresh() == 1
    assert "p2" in u.env.caps.data and "p1" not in u.env.caps.data


async def test_relay_outbox_publishes_once(uc):
    u = uc(FakeProvider("p1", [failure(FailureClass.INVALID_REQUEST)]))
    await u.create(cmd())
    pub = FakePublisher()
    relay = RelayOutbox(uow_factory=u.env.uow, publisher=pub)
    assert await relay() == 1
    assert await relay() == 0
    assert [t for t, _ in pub.published] == ["PaymentFailed"]
