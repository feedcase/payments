import asyncio
import json
from uuid import uuid4

import pytest
import redis.asyncio as aioredis

from modules.payments.application.ports import ProviderCapabilities
from modules.payments.config import BreakerSettings
from modules.payments.domain.value_objects import Money, ProviderCode
from modules.payments.infrastructure.redis.breaker import RedisCircuitBreaker
from modules.payments.infrastructure.redis.capabilities import RedisCapabilityCache
from modules.payments.infrastructure.redis.idempotency import RedisIdempotencyLock, RedisRateLimiter
from modules.payments.infrastructure.redis.publisher import RedisStreamPublisher
from tests.integration.conftest import TEST_REDIS_URL

pytestmark = pytest.mark.integration
NP = ProviderCode("nowpayments")


@pytest.fixture
async def r():
    client = aioredis.from_url(TEST_REDIS_URL, decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()


def breaker(r, **kw):
    return RedisCircuitBreaker(r, BreakerSettings(**kw))


async def test_breaker_opens_after_threshold(r):
    b = breaker(r)
    for i in range(20):
        await b.record(NP, ok=i % 2 == 0)
    assert not await b.is_available(NP)


async def test_breaker_stays_closed_under_min_calls(r):
    b = breaker(r)
    for _ in range(19):
        await b.record(NP, ok=False)
    assert await b.is_available(NP)


async def test_breaker_stays_closed_below_ratio(r):
    b = breaker(r)
    for i in range(30):
        await b.record(NP, ok=i % 3 != 0)  # 10/30 fail < 50%
    assert await b.is_available(NP)


async def test_breaker_half_open_single_probe(r):
    b = breaker(r, open_s=1)
    for _ in range(20):
        await b.record(NP, ok=False)
    assert not await b.is_available(NP)
    await asyncio.sleep(1.1)
    assert await b.is_available(NP)  # пробный вызов
    assert not await b.is_available(NP)  # второй — ждёт probe_interval


async def test_breaker_probe_success_closes(r):
    b = breaker(r, open_s=1)
    for _ in range(20):
        await b.record(NP, ok=False)
    await asyncio.sleep(1.1)
    assert await b.is_available(NP)
    await b.record(NP, ok=True)
    assert await b.is_available(NP)
    assert await b.is_available(NP)


async def test_breaker_probe_failure_reopens(r):
    b = breaker(r, open_s=1)
    for _ in range(20):
        await b.record(NP, ok=False)
    await asyncio.sleep(1.1)
    assert await b.is_available(NP)
    await b.record(NP, ok=False)
    assert not await b.is_available(NP)


async def test_breaker_isolated_per_provider(r):
    b = breaker(r)
    for _ in range(20):
        await b.record(NP, ok=False)
    assert await b.is_available(ProviderCode("coingate"))


async def test_cascade_works_when_redis_down():
    dead = aioredis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2)
    b = RedisCircuitBreaker(dead, BreakerSettings())
    assert await b.is_available(NP)
    await b.record(NP, ok=False)
    caps = RedisCapabilityCache(dead)
    assert await caps.get(NP) is None
    await dead.aclose()


async def test_caps_roundtrip_and_ttl(r):
    c = RedisCapabilityCache(r, ttl_s=900)
    caps = ProviderCapabilities(
        currencies=frozenset({"btc", "usdttrc20"}),
        min_amount={"btc": Money(amount="5", currency="usd")},
    )
    await c.set(NP, caps)
    assert await c.get(NP) == caps
    assert 890 < await r.ttl("pay:cap:nowpayments") <= 900


async def test_lock_exclusive(r):
    lock = RedisIdempotencyLock(r, ttl_s=60)
    u = uuid4()
    assert await lock.acquire(u, "k")
    assert not await lock.acquire(u, "k")
    assert await lock.acquire(uuid4(), "k")
    await lock.release(u, "k")
    assert await lock.acquire(u, "k")


async def test_rate_limit(r):
    rl = RedisRateLimiter(r, per_min=3)
    u = uuid4()
    assert [await rl.allow(u) for _ in range(4)] == [True, True, True, False]
    assert await rl.allow(uuid4())


async def test_publisher_xadd(r):
    pub = RedisStreamPublisher(r, stream="payments.events")
    await pub.publish("PaymentSucceeded", {"payment_id": "p1"})
    (entry,) = await r.xrange("payments.events")
    assert entry[1]["type"] == "PaymentSucceeded"
    assert json.loads(entry[1]["payload"]) == {"payment_id": "p1"}
