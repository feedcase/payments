import hashlib
import hmac
import json
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
import respx

from modules.payments.application.ports import (
    CreateProviderPayment,
    InvalidWebhook,
    ProviderFailure,
)
from modules.payments.config import NowPaymentsSettings
from modules.payments.domain.errors import FailureClass
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.value_objects import Money
from modules.payments.infrastructure.providers.nowpayments import NowPaymentsProvider

BASE = "https://api-sandbox.nowpayments.io/v1"


def provider(client, **kw):
    s = NowPaymentsSettings(enabled=True, api_key="key", ipn_secret="ipn", **kw)
    return NowPaymentsProvider(s, client)


def cmd():
    return CreateProviderPayment(
        attempt_id=uuid4(),
        price=Money(amount="9.99", currency="USD"),
        pay_currency="usdttrc20",
        callback_url="http://cb/nowpayments",
        timeout_s=5,
    )


CREATED = {
    "payment_id": 5077125051,
    "payment_status": "waiting",
    "pay_address": "TXYZ",
    "pay_amount": 9.99,
    "pay_currency": "usdttrc20",
    "expiration_estimate_date": "2026-11-12T12:30:00.000Z",
}


@respx.mock
async def test_np_create_sends_strings_not_floats():
    route = respx.post(f"{BASE}/payment").mock(return_value=httpx.Response(201, json=CREATED))
    c = cmd()
    async with httpx.AsyncClient() as client:
        res = await provider(client).create(c)
    req = route.calls[0].request
    body = json.loads(req.content)
    assert req.headers["x-api-key"] == "key"
    assert body["price_amount"] == "9.99" and body["price_currency"] == "usd"
    assert body["order_id"] == str(c.attempt_id)
    assert body["ipn_callback_url"] == "http://cb/nowpayments"
    assert "case" not in body
    assert res.provider_payment_id == "5077125051"
    assert res.checkout.pay_address == "TXYZ"
    assert res.checkout.pay_amount == Decimal("9.99")
    assert res.checkout.expires_at.isoformat().startswith("2026-11-12T12:30")


@respx.mock
async def test_np_create_includes_case_only_in_sandbox():
    route = respx.post(f"{BASE}/payment").mock(return_value=httpx.Response(201, json=CREATED))
    async with httpx.AsyncClient() as client:
        await provider(client, sandbox_case="failure").create(cmd())
    assert json.loads(route.calls[0].request.content)["case"] == "failure"


@respx.mock
async def test_np_create_without_expiration_uses_ttl():
    body = {k: v for k, v in CREATED.items() if k != "expiration_estimate_date"}
    respx.post(f"{BASE}/payment").mock(return_value=httpx.Response(201, json=body))
    async with httpx.AsyncClient() as client:
        res = await provider(client).create(cmd())
    assert res.checkout.expires_at is not None


@respx.mock
async def test_np_unsupported_min_amount():
    respx.post(f"{BASE}/payment").mock(
        return_value=httpx.Response(
            400, json={"statusCode": 400, "code": "AMOUNT_MINIMAL_ERROR", "message": "too small"}
        )
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(ProviderFailure) as e:
            await provider(client).create(cmd())
    assert e.value.failure.cls is FailureClass.UNSUPPORTED


@respx.mock
async def test_np_unsupported_currency_message():
    respx.post(f"{BASE}/payment").mock(
        return_value=httpx.Response(
            400, json={"code": "INVALID_REQUEST_PARAMS", "message": "Currency xyz is not available"}
        )
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(ProviderFailure) as e:
            await provider(client).create(cmd())
    assert e.value.failure.cls is FailureClass.UNSUPPORTED


@pytest.mark.parametrize(
    ("np_status", "expected"),
    [
        ("waiting", A.CREATED),
        ("confirming", A.CONFIRMING),
        ("confirmed", A.CONFIRMING),
        ("sending", A.CONFIRMING),
        ("partially_paid", A.PARTIALLY_PAID),
        ("finished", A.FINISHED),
        ("failed", A.FAILED),
        ("refunded", A.FAILED),
        ("expired", A.EXPIRED),
    ],
)
@respx.mock
async def test_np_status_mapping(np_status, expected):
    respx.get(f"{BASE}/payment/1").mock(
        return_value=httpx.Response(
            200,
            json={
                "payment_id": 1,
                "payment_status": np_status,
                "actually_paid": 9.5,
                "pay_currency": "usdttrc20",
            },
        )
    )
    async with httpx.AsyncClient() as client:
        st = await provider(client).fetch("1")
    assert st.status is expected
    assert st.received == Money(amount="9.5", currency="usdttrc20")


@respx.mock
async def test_np_fetch_404_none():
    respx.get(f"{BASE}/payment/1").mock(return_value=httpx.Response(404, json={}))
    async with httpx.AsyncClient() as client:
        assert await provider(client).fetch("1") is None


def sign(payload: dict, secret="ipn") -> str:
    msg = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), msg, hashlib.sha512).hexdigest()


async def test_np_webhook_signature_ok():
    oid = uuid4()
    payload = {
        "payment_id": 77,
        "payment_status": "finished",
        "order_id": str(oid),
        "b": {"z": 1, "a": 2},
    }
    body = json.dumps(payload).encode()
    async with httpx.AsyncClient() as client:
        ref = provider(client).parse_webhook({"x-nowpayments-sig": sign(payload)}, body)
    assert ref.provider_payment_id == "77" and ref.order_id == oid


async def test_np_webhook_bad_signature_raises():
    payload = {"payment_id": 77, "order_id": str(uuid4())}
    async with httpx.AsyncClient() as client:
        p = provider(client)
        with pytest.raises(InvalidWebhook):
            p.parse_webhook(
                {"x-nowpayments-sig": sign(payload, "other")}, json.dumps(payload).encode()
            )
        with pytest.raises(InvalidWebhook):
            p.parse_webhook({}, json.dumps(payload).encode())
        with pytest.raises(InvalidWebhook):
            p.parse_webhook({"x-nowpayments-sig": "x"}, b"not json")


@respx.mock
async def test_np_capabilities():
    respx.get(f"{BASE}/currencies").mock(
        return_value=httpx.Response(200, json={"currencies": ["btc", "usdttrc20"]})
    )
    respx.get(f"{BASE}/min-amount").mock(
        return_value=httpx.Response(
            200, json={"currency_from": "btc", "min_amount": 0.0001, "fiat_equivalent": 7.5}
        )
    )
    async with httpx.AsyncClient() as client:
        caps = await provider(client).capabilities(["btc"])
    assert caps.currencies == frozenset({"btc", "usdttrc20"})
    assert caps.min_amount["btc"] == Money(amount="7.5", currency="usd")


@respx.mock
async def test_np_health():
    respx.get(f"{BASE}/status").mock(return_value=httpx.Response(200, json={"message": "OK"}))
    async with httpx.AsyncClient() as client:
        assert await provider(client).health()
