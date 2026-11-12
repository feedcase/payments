"""Общий HTTP-вызов провайдера и перевод ошибок в доменные классы (таблица 2.4)."""

import json
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx

from modules.payments.application.ports import ProviderFailure
from modules.payments.domain.errors import AttemptFailure
from modules.payments.domain.errors import FailureClass as F

UnsupportedPredicate = Callable[[int, dict[str, Any]], bool]

_UNAVAILABLE_EXC = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)
# запрос мог дойти до провайдера — исход неизвестен
_UNKNOWN_EXC = (httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError)
_UNAVAILABLE_STATUS = {429, 502, 503, 504}


def _fail(cls: F, code: str, message: str = "") -> ProviderFailure:
    return ProviderFailure(AttemptFailure(cls=cls, code=code, message=message[:500]))


def _json_or_empty(resp: httpx.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {"data": data}


async def request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    timeout_s: float,
    connect_timeout_s: float = 2.0,
    is_unsupported: UnsupportedPredicate | None = None,
    allow_404: bool = False,
    **kwargs: Any,
) -> Any:
    timeout = httpx.Timeout(
        timeout_s, connect=min(connect_timeout_s, timeout_s), pool=connect_timeout_s
    )
    try:
        resp = await client.request(method, url, timeout=timeout, **kwargs)
    except _UNAVAILABLE_EXC as e:
        raise _fail(F.PROVIDER_UNAVAILABLE, type(e).__name__, str(e)) from e
    except _UNKNOWN_EXC as e:
        raise _fail(F.TIMEOUT_UNKNOWN, type(e).__name__, str(e)) from e
    except httpx.TransportError as e:
        raise _fail(F.PROVIDER_UNAVAILABLE, type(e).__name__, str(e)) from e

    status = resp.status_code
    if status == 404 and allow_404:
        return None
    if status >= 400:
        body = _json_or_empty(resp)
        code = f"http_{status}"
        msg = resp.text
        if status in _UNAVAILABLE_STATUS:
            raise _fail(F.PROVIDER_UNAVAILABLE, code, msg)
        if status >= 500:
            raise _fail(F.PROVIDER_ERROR, code, msg)
        if status in (401, 403):
            raise _fail(F.INVALID_REQUEST, code, msg)
        if is_unsupported is not None and is_unsupported(status, body):
            raise _fail(F.UNSUPPORTED, code, msg)
        raise _fail(F.INVALID_REQUEST, code, msg)
    try:
        return resp.json()
    except (json.JSONDecodeError, ValueError) as e:
        raise _fail(F.PROVIDER_ERROR, "invalid_json", resp.text) from e


def dec(value: Any) -> Decimal | None:
    """JSON-числа провайдеров приходят float'ами — переводим через str без потери вида."""
    if value is None or value == "":
        return None
    return Decimal(str(value))


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
