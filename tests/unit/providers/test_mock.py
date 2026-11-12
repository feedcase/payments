import hashlib
import hmac
import json
from uuid import uuid4

import httpx
import pytest

from mock_provider.app import create_app
from modules.payments.application.ports import (
    CreateProviderPayment,
    InvalidWebhook,
    ProviderFailure,
)
from modules.payments.config import (
    CascadeSettings,
    CoinGateSettings,
    MockSettings,
    NowPaymentsSettings,
)
from modules.payments.domain.errors import FailureClass
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.value_objects import Money
from modules.payments.infrastructure.providers.mock import MockProvider
from modules.payments.infrastructure.providers.registry import build_registry


@pytest.fixture
async def client(monkeypatch):
    monkeypatch.setenv("MOCK_WEBHOOK_DELAY_S", "0")
    app = create_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock"
    ) as c:
        yield c


def provider(client, name="mock_a"):
    return MockProvider(code=name, base_url=f"http://mock/{name}", secret="dev", client=client)


def cmd():
    return CreateProviderPayment(
        attempt_id=uuid4(),
        price=Money(amount="9.99", currency="USD"),
        pay_currency="usdttrc20",
        callback_url="http://nowhere.invalid/webhooks/mock_a",
        timeout_s=5,
    )


async def scenario(client, name, s):
    r = await client.post(f"/{name}/_control/scenario", json={"scenario": s})
    assert r.status_code == 200


async def test_mock_ok_flow(client):
    p = provider(client)
    await scenario(client, "mock_a", "manual")
    res = await p.create(cmd())
    assert res.checkout.pay_address
    st = await p.fetch(res.provider_payment_id)
    assert st.status is A.CREATED


async def test_mock_manual_pay_moves_status(client):
    p = provider(client)
    await scenario(client, "mock_a", "manual")
    res = await p.create(cmd())
    r = await client.post(
        f"/mock_a/_control/pay/{res.provider_payment_id}", params={"status": "finished"}
    )
    assert r.status_code == 200
    st = await p.fetch(res.provider_payment_id)
    assert st.status is A.FINISHED and st.received == Money(amount="9.99", currency="usdttrc20")


async def test_mock_http_500_maps_provider_error(client):
    await scenario(client, "mock_a", "http_500")
    with pytest.raises(ProviderFailure) as e:
        await provider(client).create(cmd())
    assert e.value.failure.cls is FailureClass.PROVIDER_ERROR


async def test_mock_unsupported(client):
    await scenario(client, "mock_a", "unsupported")
    with pytest.raises(ProviderFailure) as e:
        await provider(client).create(cmd())
    assert e.value.failure.cls is FailureClass.UNSUPPORTED


async def test_mock_instances_isolated(client):
    await scenario(client, "mock_a", "http_500")
    await scenario(client, "mock_b", "manual")
    res = await provider(client, "mock_b").create(cmd())
    assert res.provider_payment_id.startswith("mock_b-")


async def test_mock_ghost_success_creates_but_500(client):
    await scenario(client, "mock_a", "ghost_success")
    c = cmd()
    with pytest.raises(ProviderFailure):
        await provider(client).create(c)
    listed = (await client.get("/mock_a/_control/payments")).json()
    assert [x["order_id"] for x in listed] == [str(c.attempt_id)]


async def test_mock_webhook_signature(client):
    p = provider(client)
    body = json.dumps({"id": "mock_a-1", "order_id": str(uuid4()), "status": "finished"}).encode()
    sig = hmac.new(b"dev", body, hashlib.sha256).hexdigest()
    ref = p.parse_webhook({"x-mock-sig": sig}, body)
    assert ref.provider_payment_id == "mock_a-1"
    with pytest.raises(InvalidWebhook):
        p.parse_webhook({"x-mock-sig": "bad"}, body)


async def test_registry_only_enabled():
    registry, clients = build_registry(
        cascade=CascadeSettings(),
        nowpayments=NowPaymentsSettings(enabled=False),
        coingate=CoinGateSettings(enabled=True, api_token="t", callback_secret="s"),
        mocks=[MockSettings(code="mock_a", base_url="http://m/mock_a", secret="s")],
    )
    try:
        assert {p.code for p in registry.all()} == {"coingate", "mock_a"}
        assert registry.has("coingate") and not registry.has("nowpayments")
        assert len(clients) == 2
    finally:
        for c in clients:
            await c.aclose()


async def test_mock_ids_unique_across_restarts(monkeypatch):
    monkeypatch.setenv("MOCK_DEFAULT_SCENARIO", "manual")
    ids = []
    for _ in range(2):  # два «запуска» сервиса
        app = create_app()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://mock"
        ) as c:
            ids.append((await provider(c).create(cmd())).provider_payment_id)
    assert ids[0] != ids[1]
