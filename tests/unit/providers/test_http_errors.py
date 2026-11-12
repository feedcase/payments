import httpx
import pytest
import respx

from modules.payments.application.ports import ProviderFailure
from modules.payments.domain.errors import FailureClass as F
from modules.payments.infrastructure.providers._http import request

URL = "https://p.test/x"


def unsupported(status, body):
    return body.get("code") == "UNSUPPORTED"


async def call():
    async with httpx.AsyncClient() as c:
        return await request(c, "POST", URL, timeout_s=1, is_unsupported=unsupported, json={})


@pytest.mark.parametrize(
    ("exc", "cls"),
    [
        (httpx.ConnectError("x"), F.PROVIDER_UNAVAILABLE),
        (httpx.ConnectTimeout("x"), F.PROVIDER_UNAVAILABLE),
        (httpx.PoolTimeout("x"), F.PROVIDER_UNAVAILABLE),
        (httpx.ReadTimeout("x"), F.TIMEOUT_UNKNOWN),
        (httpx.WriteTimeout("x"), F.TIMEOUT_UNKNOWN),
        (httpx.RemoteProtocolError("x"), F.TIMEOUT_UNKNOWN),
    ],
)
@respx.mock
async def test_transport_errors(exc, cls):
    respx.post(URL).mock(side_effect=exc)
    with pytest.raises(ProviderFailure) as e:
        await call()
    assert e.value.failure.cls is cls
    assert e.value.failure.code == type(exc).__name__


@pytest.mark.parametrize(
    ("status", "body", "cls"),
    [
        (429, {}, F.PROVIDER_UNAVAILABLE),
        (502, {}, F.PROVIDER_UNAVAILABLE),
        (503, {}, F.PROVIDER_UNAVAILABLE),
        (504, {}, F.PROVIDER_UNAVAILABLE),
        (500, {"message": "boom"}, F.PROVIDER_ERROR),
        (501, {}, F.PROVIDER_ERROR),
        (401, {}, F.INVALID_REQUEST),
        (403, {}, F.INVALID_REQUEST),
        (400, {"code": "UNSUPPORTED"}, F.UNSUPPORTED),
        (422, {"code": "UNSUPPORTED"}, F.UNSUPPORTED),
        (400, {"code": "BAD"}, F.INVALID_REQUEST),
        (404, {}, F.INVALID_REQUEST),
    ],
)
@respx.mock
async def test_status_mapping(status, body, cls):
    respx.post(URL).mock(return_value=httpx.Response(status, json=body))
    with pytest.raises(ProviderFailure) as e:
        await call()
    assert e.value.failure.cls is cls
    assert e.value.failure.code == f"http_{status}"


@respx.mock
async def test_invalid_json_2xx_is_provider_error():
    respx.post(URL).mock(return_value=httpx.Response(200, text="<html>"))
    with pytest.raises(ProviderFailure) as e:
        await call()
    assert e.value.failure.cls is F.PROVIDER_ERROR


@respx.mock
async def test_non_json_error_body_still_classified():
    respx.post(URL).mock(return_value=httpx.Response(400, text="bad"))
    with pytest.raises(ProviderFailure) as e:
        await call()
    assert e.value.failure.cls is F.INVALID_REQUEST


@respx.mock
async def test_ok_returns_json():
    respx.post(URL).mock(return_value=httpx.Response(201, json={"a": 1}))
    assert await call() == {"a": 1}


@respx.mock
async def test_allow_404_returns_none():
    respx.get(URL).mock(return_value=httpx.Response(404, json={}))
    async with httpx.AsyncClient() as c:
        assert await request(c, "GET", URL, timeout_s=1, allow_404=True) is None
