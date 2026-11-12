from modules.payments.application.ports import ProviderCapabilities
from modules.payments.domain.errors import AttemptFailure, FailureClass
from modules.payments.domain.value_objects import Money, ProviderCode
from tests.fakes import FakeProvider

P1, P2 = ProviderCode("p1"), ProviderCode("p2")


async def route(env, payment):
    async with env.uow() as uow:
        return await env.router.next(uow, payment)


async def test_router_order_from_route(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"), routes={"*": ["p2", "p1"]})
    p = await env.new_payment()
    assert await route(env, p) == P2


async def test_router_skips_unsupported_currency_when_cached(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    env.caps.data["p1"] = ProviderCapabilities(currencies=frozenset({"btc"}))
    p = await env.new_payment(currency="usdttrc20")
    assert await route(env, p) == P2


async def test_router_keeps_provider_on_cache_miss(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    p = await env.new_payment()
    assert await route(env, p) == P1


async def test_router_skips_below_min_amount(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    env.caps.data["p1"] = ProviderCapabilities(
        currencies=frozenset({"usdttrc20"}),
        min_amount={"usdttrc20": Money(amount="20", currency="usd")},
    )
    p = await env.new_payment(amount="9.99")
    assert await route(env, p) == P2


async def test_router_ignores_min_amount_in_other_currency(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    env.caps.data["p1"] = ProviderCapabilities(
        currencies=frozenset({"usdttrc20"}),
        min_amount={"usdttrc20": Money(amount="20", currency="eur")},
    )
    p = await env.new_payment(amount="9.99")
    assert await route(env, p) == P1


async def test_router_skips_open_breaker(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    env.breaker.open.add("p1")
    p = await env.new_payment()
    assert await route(env, p) == P2


async def test_router_excludes_tried_in_round(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    p = await env.new_payment()
    a = p.start_attempt(P1, env.clock.now())
    p.apply_create_failure(
        a.id, AttemptFailure(cls=FailureClass.PROVIDER_ERROR, code="x"), env.clock.now()
    )
    assert await route(env, p) == P2


async def test_router_skips_unregistered_provider(make_env):
    env = make_env(FakeProvider("p2"))
    p = await env.new_payment()
    assert await route(env, p) == P2


async def test_router_wildcard_route(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"), routes={"btc": ["p2"], "*": ["p1"]})
    assert await route(env, await env.new_payment(currency="btc")) == P2
    assert await route(env, await env.new_payment(currency="eth")) == P1
