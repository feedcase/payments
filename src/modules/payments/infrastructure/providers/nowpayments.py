"""NOWPayments (https://documenter.getpostman.com/view/7907941/2s93JusNJt).

Параметры взяты из Postman-коллекции и сторонних SDK; перед продом — сверить.
"""

import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
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
from modules.payments.config import NowPaymentsSettings
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.value_objects import Checkout, Money, ProviderCode
from modules.payments.infrastructure.providers._http import dec, parse_dt, request

STATUS_MAP: dict[str, A] = {
    "waiting": A.CREATED,
    "confirming": A.CONFIRMING,
    "confirmed": A.CONFIRMING,
    "sending": A.CONFIRMING,
    "partially_paid": A.PARTIALLY_PAID,
    "finished": A.FINISHED,
    "failed": A.FAILED,
    "refunded": A.FAILED,
    "expired": A.EXPIRED,
}
_UNSUPPORTED_CODES = {"AMOUNT_MINIMAL_ERROR", "CURRENCY_UNAVAILABLE"}


def _is_unsupported(status: int, body: dict[str, Any]) -> bool:
    if body.get("code") in _UNSUPPORTED_CODES:
        return True
    msg = str(body.get("message", "")).lower()
    return "currency" in msg and ("not" in msg or "unavailable" in msg)


class NowPaymentsProvider:
    code = ProviderCode("nowpayments")

    def __init__(
        self,
        settings: NowPaymentsSettings,
        client: httpx.AsyncClient,
        invoice_ttl_min: int = 30,
        read_timeout_s: float = 8.0,
    ) -> None:
        self._s = settings
        self._c = client
        self._ttl = timedelta(minutes=invoice_ttl_min)
        self._read = read_timeout_s

    def _url(self, path: str) -> str:
        return f"{self._s.base_url.rstrip('/')}{path}"

    @property
    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self._s.api_key}

    async def create(self, cmd: CreateProviderPayment) -> ProviderCreateResult:
        body: dict[str, Any] = {
            "price_amount": str(cmd.price.amount),
            "price_currency": cmd.price.currency,
            "pay_currency": cmd.pay_currency,
            "order_id": str(cmd.attempt_id),
            "ipn_callback_url": cmd.callback_url,
        }
        if self._s.sandbox_case:
            body["case"] = self._s.sandbox_case
        data = await request(
            self._c,
            "POST",
            self._url("/payment"),
            timeout_s=cmd.timeout_s,
            is_unsupported=_is_unsupported,
            json=body,
            headers=self._headers,
        )
        expires = parse_dt(data.get("expiration_estimate_date")) or (datetime.now(UTC) + self._ttl)
        return ProviderCreateResult(
            provider_payment_id=str(data["payment_id"]),
            checkout=Checkout(
                pay_address=data.get("pay_address"),
                pay_amount=dec(data.get("pay_amount")),
                pay_currency=data.get("pay_currency") or cmd.pay_currency,
                expires_at=expires,
            ),
        )

    async def fetch(self, provider_payment_id: str) -> ProviderPaymentState | None:
        data = await request(
            self._c,
            "GET",
            self._url(f"/payment/{provider_payment_id}"),
            timeout_s=self._read,
            allow_404=True,
            headers=self._headers,
        )
        if data is None:
            return None
        status = STATUS_MAP.get(str(data.get("payment_status")))
        if status is None:
            return None
        paid = dec(data.get("actually_paid"))
        received = (
            Money(amount=paid, currency=str(data.get("pay_currency")))
            if paid is not None and data.get("pay_currency")
            else None
        )
        return ProviderPaymentState(status=status, received=received)

    def parse_webhook(self, headers: Mapping[str, str], body: bytes) -> ProviderEventRef:
        sig = headers.get("x-nowpayments-sig")
        if not sig or not self._s.ipn_secret:
            raise InvalidWebhook("missing signature")
        try:
            payload = json.loads(body)
        except ValueError as e:
            raise InvalidWebhook("invalid json") from e
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        expected = hmac.new(self._s.ipn_secret.encode(), canonical, hashlib.sha512).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise InvalidWebhook("bad signature")
        order_id = payload.get("order_id")
        try:
            oid = UUID(str(order_id)) if order_id else None
        except ValueError:
            oid = None
        pid = payload.get("payment_id")
        return ProviderEventRef(provider_payment_id=str(pid) if pid else None, order_id=oid)

    async def capabilities(self, pay_currencies: Sequence[str]) -> ProviderCapabilities:
        data = await request(
            self._c, "GET", self._url("/currencies"), timeout_s=self._read, headers=self._headers
        )
        currencies = frozenset(str(c).lower() for c in data.get("currencies", []))
        mins: dict[str, Money] = {}
        for cur in pay_currencies:
            m = await request(
                self._c,
                "GET",
                self._url("/min-amount"),
                timeout_s=self._read,
                headers=self._headers,
                params={"currency_from": cur, "currency_to": cur, "fiat_equivalent": "usd"},
            )
            fiat = dec(m.get("fiat_equivalent"))
            if fiat is not None:
                mins[cur] = Money(amount=fiat, currency="usd")
        return ProviderCapabilities(currencies=currencies, min_amount=mins)

    async def health(self) -> bool:
        data = await request(self._c, "GET", self._url("/status"), timeout_s=self._read)
        return bool(data) and data.get("message") == "OK"
