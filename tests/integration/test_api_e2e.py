import asyncio
import hashlib
import hmac
import json
from uuid import uuid4

import httpx
import jwt
import pytest
import redis.asyncio as aioredis
from sqlalchemy import text

from app.main import create_app
from app.settings import DbSettings, JwtSettings, RedisSettings, Settings
from mock_provider.app import create_app as create_mock
from modules.payments.config import MockSettings
from modules.payments.container import Container
from tests.integration.conftest import TEST_DB_URL, TEST_REDIS_URL

pytestmark = pytest.mark.integration
SECRET = "e2e-secret-at-least-32-bytes-long!!!"
BODY = {
    "price": {"amount": "9.99", "currency": "USD"},
    "pay_currency": "usdttrc20",
    "purpose": {"domain": "x", "ref": "1"},
}


@pytest.fixture
async def stack(sessionmaker, monkeypatch):
    monkeypatch.setenv("MOCK_DEFAULT_SCENARIO", "manual")
    r = aioredis.from_url(TEST_REDIS_URL)
    await r.flushdb()
    await r.aclose()
    async with sessionmaker() as s:
        await s.execute(
            text(
                "INSERT INTO cascade_routes(method, pay_currency, providers) VALUES "
                "('CRYPTO','usdttrc20', ARRAY['mock_primary','mock_backup'])"
            )
        )
        await s.commit()
    mock = create_mock()
    mock_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock), base_url="http://mock")
    settings = Settings(
        db=DbSettings(url=TEST_DB_URL),
        redis=RedisSettings(url=TEST_REDIS_URL),
        jwt=JwtSettings(secret=SECRET),
        mocks=[
            MockSettings(code="mock_primary", base_url="http://mock/mock_primary", secret="dev"),
            MockSettings(code="mock_backup", base_url="http://mock/mock_backup", secret="dev"),
        ],
        task_queue="inline",
    )
    container = Container(settings, http_transport=httpx.ASGITransport(app=mock))
    await container.startup()
    app = create_app(container)
    api = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api")
    yield api, mock_client, sessionmaker, container
    await api.aclose()
    await mock_client.aclose()
    await container.shutdown()


def headers(key: str, user=None):
    tok = jwt.encode({"sub": str(user or USER)}, SECRET, algorithm="HS256")
    return {"Authorization": f"Bearer {tok}", "Idempotency-Key": key}


USER = uuid4()


async def test_e2e_mock_primary_500_backup_ok(stack):
    api, mock, _, _ = stack
    await mock.post("/mock_primary/_control/scenario", json={"scenario": "http_500"})
    r = await api.post("/api/v1/payments", json=BODY, headers=headers("e2e-1"))
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["status"] == "AWAITING_CUSTOMER"
    assert data["attempt"]["provider"] == "mock_backup" and data["attempt"]["seq"] == 2


async def test_idem_concurrent_same_body_one_payment(stack):
    api, _, sessionmaker, _ = stack
    rs = await asyncio.gather(
        *(api.post("/api/v1/payments", json=BODY, headers=headers("same-key")) for _ in range(10))
    )
    assert {r.status_code for r in rs} <= {200, 201}
    assert len({r.json()["payment_id"] for r in rs}) == 1
    async with sessionmaker() as s:
        assert (await s.execute(text("SELECT count(*) FROM payments"))).scalar() == 1
        assert (await s.execute(text("SELECT count(*) FROM payment_attempts"))).scalar() == 1


async def test_e2e_webhook_marks_succeeded(stack):
    api, mock, _, container = stack
    r = await api.post("/api/v1/payments", json=BODY, headers=headers("wh-1"))
    pid, attempt = r.json()["payment_id"], r.json()["attempt"]
    provider_payment = (await mock.get("/mock_primary/_control/payments")).json()[0]
    await mock.post(
        f"/mock_primary/_control/pay/{provider_payment['id']}", params={"status": "finished"}
    )
    body = json.dumps(
        {"id": provider_payment["id"], "order_id": attempt["attempt_id"], "status": "finished"}
    ).encode()
    sig = hmac.new(b"dev", body, hashlib.sha256).hexdigest()
    wr = await api.post("/webhooks/mock_primary", content=body, headers={"x-mock-sig": sig})
    assert wr.status_code == 200
    await container.queue.drain()
    got = await api.get(f"/api/v1/payments/{pid}", headers=headers("wh-1"))
    assert got.json()["status"] == "SUCCEEDED"
    assert got.json()["received_total"] == "9.99"


async def test_e2e_recover_orphaned_creating(stack):
    """Review C1: процесс упал между транзакциями шага — sweeper доводит каскад."""
    from datetime import UTC, datetime, timedelta

    from modules.payments.domain.entities import Payment
    from modules.payments.domain.value_objects import Money, ProviderCode, Purpose

    api, _, _sm, container = stack
    old = datetime.now(UTC) - timedelta(minutes=5)
    p = Payment.create(
        user_id=USER,
        price=Money(amount="9.99", currency="USD"),
        pay_currency="usdttrc20",
        purpose=Purpose(domain="x", ref="1"),
        idempotency_key="orphan",
        request_hash="h",
        now=old,
    )
    p.start_attempt(ProviderCode("mock_primary"), old)
    async with container.uow_factory() as uow:
        await uow.payments.add(p)
        await uow.commit()
    assert await container.recover() >= 1
    got = (await api.get(f"/api/v1/payments/{p.id}", headers=headers("orphan"))).json()
    assert got["status"] == "AWAITING_CUSTOMER"
    assert got["attempt"]["provider"] == "mock_backup" and got["attempt"]["seq"] == 2
