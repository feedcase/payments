"""CoinGate API v2 (https://developer.coingate.com/reference).

TODO: эндпоинты и поля взяты из сторонних источников — сверить с developer.coingate.com.
"""

import hashlib
import hmac
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs
from uuid import UUID

import httpx

from modules.payments.application.ports import (
    CreateProviderPayment,
    InvalidWebhook,
    ProviderCapabilities,
    ProviderCreateResult,
    ProviderEventRef,
    ProviderPaymentState,
)
from modules.payments.config import CoinGateSettings
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.value_objects import Checkout, Money, ProviderCode
from modules.payments.infrastructure.providers._http import dec, parse_dt, request

STATUS_MAP: dict[str, A] = {
    "new": A.CREATED,
    "pending": A.CREATED,
    "confirming": A.CONFIRMING,
    "paid": A.FINISHED,
    "invalid": A.FAILED,
    "canceled": A.FAILED,
    "refunded": A.FAILED,
    "expired": A.EXPIRED,
}


def _is_unsupported(status: int, body: dict[str, Any]) -> bool:
    text = f"{body.get('reason', '')} {body.get('message', '')}".lower()
    return "currency" in text


class CoinGateProvider:
    code = ProviderCode("coingate")

    def __init__(
        self,
        settings: CoinGateSettings,
        client: httpx.AsyncClient,
        invoice_ttl_min: int = 30,
        read_timeout_s: float = 8.0,
    ) -> None:
        self._s = settings
        self._c = client
        self._ttl = timedelta(minutes=invoice_ttl_min)
        self._read = read_timeout_s
        self._reverse = {v.lower(): k for k, v in settings.pay_currency_map.items()}

    def _url(self, path: str) -> str:
        return f"{self._s.base_url.rstrip('/')}{path}"

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Token {self._s.api_token}"}

    def _token(self, order_id: str) -> str:
        return hmac.new(
            self._s.callback_secret.encode(), order_id.encode(), hashlib.sha256
        ).hexdigest()

    async def create(self, cmd: CreateProviderPayment) -> ProviderCreateResult:
        order_id = str(cmd.attempt_id)
        body = {
            "order_id": order_id,
            "price_amount": str(cmd.price.amount),
            "price_currency": cmd.price.currency.upper(),
            "receive_currency": self._s.receive_currency,
            "pay_currency": self._s.pay_currency_map.get(
                cmd.pay_currency, cmd.pay_currency.upper()
            ),
            "callback_url": cmd.callback_url,
            "token": self._token(order_id),
        }
        data = await request(
            self._c,
            "POST",
            self._url("/orders"),
            timeout_s=cmd.timeout_s,
            is_unsupported=_is_unsupported,
            json=body,
            headers=self._headers,
        )
        expires = parse_dt(data.get("expire_at")) or (datetime.now(UTC) + self._ttl)
        return ProviderCreateResult(
            provider_payment_id=str(data["id"]),
            checkout=Checkout(invoice_url=data.get("payment_url"), expires_at=expires),
        )

    async def fetch(self, provider_payment_id: str) -> ProviderPaymentState | None:
        data = await request(
            self._c,
            "GET",
            self._url(f"/orders/{provider_payment_id}"),
            timeout_s=self._read,
            allow_404=True,
            headers=self._headers,
        )
        if data is None:
            return None
        status = STATUS_MAP.get(str(data.get("status")))
        if status is None:
            return None
        received = None
        amount = dec(data.get("pay_amount"))
        if status is A.FINISHED and amount is not None and data.get("pay_currency"):
            received = Money(amount=amount, currency=str(data["pay_currency"]))
        return ProviderPaymentState(status=status, received=received)

    def parse_webhook(self, headers: Mapping[str, str], body: bytes) -> ProviderEventRef:
        form = {k: v[0] for k, v in parse_qs(body.decode(errors="replace")).items()}
        order_id, token = form.get("order_id"), form.get("token")
        if not order_id or not token or not self._s.callback_secret:
            raise InvalidWebhook("missing order_id/token")
        if not hmac.compare_digest(self._token(order_id), token):
            raise InvalidWebhook("bad token")
        try:
            oid = UUID(order_id)
        except ValueError as e:
            raise InvalidWebhook("bad order_id") from e
        return ProviderEventRef(provider_payment_id=form.get("id"), order_id=oid)

    async def capabilities(self, pay_currencies: Sequence[str]) -> ProviderCapabilities:
        data = await request(
            self._c,
            "GET",
            self._url("/currencies"),
            timeout_s=self._read,
            params={"native": "true", "enabled": "true", "merchant_pay": "true"},
        )
        items = data if isinstance(data, list) else data.get("data", [])
        symbols = {str(i.get("symbol", "")).lower() for i in items if i.get("symbol")}
        ours = {self._reverse[s] for s in symbols if s in self._reverse}
        return ProviderCapabilities(currencies=frozenset(symbols | ours))

    async def health(self) -> bool:
        data = await request(self._c, "GET", self._url("/ping"), timeout_s=self._read)
        return bool(data)
