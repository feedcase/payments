import hashlib
import hmac
import json
from urllib.parse import urlencode
from uuid import uuid4

import httpx
import pytest
import respx

from modules.payments.application.ports import (
    CreateProviderPayment,
    InvalidWebhook,
    ProviderFailure,
)
from modules.payments.config import CoinGateSettings
from modules.payments.domain.errors import FailureClass
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.value_objects import Money
from modules.payments.infrastructure.providers.coingate import CoinGateProvider

BASE = "https://api-sandbox.coingate.com/v2"


def provider(client):
    return CoinGateProvider(
        CoinGateSettings(enabled=True, api_token="tok", callback_secret="cbs"), client
    )


def token(oid) -> str:
    return hmac.new(b"cbs", str(oid).encode(), hashlib.sha256).hexdigest()


def cmd():
    return CreateProviderPayment(
        attempt_id=uuid4(),
        price=Money(amount="9.99", currency="USD"),
        pay_currency="usdttrc20",
        callback_url="http://cb/coingate",
        timeout_s=5,
    )


@respx.mock
async def test_cg_create_auth_header_and_token():
    route = respx.post(f"{BASE}/orders").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": 1234,
                "status": "new",
                "payment_url": "https://pay-sandbox.coingate.com/invoice/abc",
                "expire_at": "2026-11-12T12:20:00+00:00",
            },
        )
    )
    c = cmd()
    async with httpx.AsyncClient() as client:
        res = await provider(client).create(c)
    req = route.calls[0].request
    body = json.loads(req.content)
    assert req.headers["authorization"] == "Token tok"
    assert body["order_id"] == str(c.attempt_id)
    assert body["price_amount"] == "9.99" and body["price_currency"] == "USD"
    assert body["receive_currency"] == "DO_NOT_CONVERT"
    assert body["pay_currency"] == "USDT"  # наш код usdttrc20 → тикер CoinGate
    assert body["callback_url"] == "http://cb/coingate"
    assert body["token"] == token(c.attempt_id)
    assert res.provider_payment_id == "1234"
    assert res.checkout.invoice_url.endswith("/invoice/abc")
    assert res.checkout.expires_at.isoformat().startswith("2026-11-12T12:20")


@pytest.mark.parametrize(
    ("cg_status", "expected"),
    [
        ("new", A.CREATED),
        ("pending", A.CREATED),
        ("confirming", A.CONFIRMING),
        ("paid", A.FINISHED),
        ("invalid", A.FAILED),
        ("canceled", A.FAILED),
        ("refunded", A.FAILED),
        ("expired", A.EXPIRED),
    ],
)
@respx.mock
async def test_cg_status_mapping(cg_status, expected):
    respx.get(f"{BASE}/orders/1").mock(
        return_value=httpx.Response(
            200, json={"id": 1, "status": cg_status, "pay_amount": "0.0001", "pay_currency": "BTC"}
        )
    )
    async with httpx.AsyncClient() as client:
        st = await provider(client).fetch("1")
    assert st.status is expected
    if expected is A.FINISHED:
        assert st.received == Money(amount="0.0001", currency="btc")
    else:
        assert st.received is None


async def test_cg_webhook_token_ok():
    oid = uuid4()
    body = urlencode(
        {"id": "1234", "order_id": str(oid), "status": "paid", "token": token(oid)}
    ).encode()
    async with httpx.AsyncClient() as client:
        ref = provider(client).parse_webhook(
            {"content-type": "application/x-www-form-urlencoded"}, body
        )
    assert ref.provider_payment_id == "1234" and ref.order_id == oid


async def test_cg_webhook_forged_token_raises():
    oid = uuid4()
    async with httpx.AsyncClient() as client:
        p = provider(client)
        forged = urlencode({"id": "1", "order_id": str(oid), "token": token(uuid4())}).encode()
        with pytest.raises(InvalidWebhook):
            p.parse_webhook({}, forged)
        with pytest.raises(InvalidWebhook):
            p.parse_webhook(
                {}, urlencode({"id": "1", "order_id": "not-a-uuid", "token": "x"}).encode()
            )
        with pytest.raises(InvalidWebhook):
            p.parse_webhook({}, b"")


@respx.mock
async def test_cg_unsupported():
    respx.post(f"{BASE}/orders").mock(
        return_value=httpx.Response(
            422, json={"message": "Order is not valid", "reason": "PayCurrencyNotSupported"}
        )
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(ProviderFailure) as e:
            await provider(client).create(cmd())
    assert e.value.failure.cls is FailureClass.UNSUPPORTED


@respx.mock
async def test_cg_capabilities_and_health():
    respx.get(f"{BASE}/currencies").mock(
        return_value=httpx.Response(200, json=[{"symbol": "BTC"}, {"symbol": "USDT"}])
    )
    respx.get(f"{BASE}/ping").mock(return_value=httpx.Response(200, json={"ping": "pong"}))
    async with httpx.AsyncClient() as client:
        p = provider(client)
        caps = await p.capabilities(["btc"])
        assert caps.currencies == frozenset({"btc", "usdt", "usdttrc20"}) and caps.min_amount == {}
        assert await p.health()
