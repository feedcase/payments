import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
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
from modules.payments.domain.value_objects import Checkout, Money, ProviderCode
from modules.payments.infrastructure.providers._http import dec, parse_dt, request
from modules.payments.infrastructure.providers.nowpayments import STATUS_MAP


def _is_unsupported(status: int, body: dict[str, object]) -> bool:
    return body.get("code") == "UNSUPPORTED"


class MockProvider:
    """Адаптер к src/mock_provider: протокол упрощённый, статусы как у NOWPayments."""

    def __init__(
        self,
        *,
        code: str,
        base_url: str,
        secret: str,
        client: httpx.AsyncClient,
        read_timeout_s: float = 8.0,
    ) -> None:
        self.code = ProviderCode(code)
        self._base = base_url.rstrip("/")
        self._secret = secret.encode()
        self._c = client
        self._read = read_timeout_s

    async def create(self, cmd: CreateProviderPayment) -> ProviderCreateResult:
        data = await request(
            self._c,
            "POST",
            f"{self._base}/payments",
            timeout_s=cmd.timeout_s,
            is_unsupported=_is_unsupported,
            json={
                "order_id": str(cmd.attempt_id),
                "price_amount": str(cmd.price.amount),
                "price_currency": cmd.price.currency,
                "pay_currency": cmd.pay_currency,
                "callback_url": cmd.callback_url,
            },
        )
        expires = parse_dt(data.get("expires_at"))
        assert expires is not None, "mock must return expires_at"
        return ProviderCreateResult(
            provider_payment_id=str(data["id"]),
            checkout=Checkout(
                pay_address=data.get("pay_address"),
                pay_amount=dec(data.get("pay_amount")),
                pay_currency=data.get("pay_currency"),
                expires_at=expires,
            ),
        )

    async def fetch(self, provider_payment_id: str) -> ProviderPaymentState | None:
        data = await request(
            self._c,
            "GET",
            f"{self._base}/payments/{provider_payment_id}",
            timeout_s=self._read,
            allow_404=True,
        )
        if data is None:
            return None
        status = STATUS_MAP.get(str(data.get("status")))
        if status is None:
            return None
        paid = dec(data.get("actually_paid"))
        received = (
            Money(amount=paid, currency=str(data["pay_currency"])) if paid is not None else None
        )
        return ProviderPaymentState(status=status, received=received)

    def parse_webhook(self, headers: Mapping[str, str], body: bytes) -> ProviderEventRef:
        expected = hmac.new(self._secret, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, headers.get("x-mock-sig", "")):
            raise InvalidWebhook("bad signature")
        try:
            payload = json.loads(body)
            oid = UUID(payload["order_id"]) if payload.get("order_id") else None
        except (ValueError, KeyError, TypeError) as e:
            raise InvalidWebhook("bad payload") from e
        return ProviderEventRef(provider_payment_id=str(payload.get("id")), order_id=oid)

    async def capabilities(self, pay_currencies: Sequence[str]) -> ProviderCapabilities:
        data = await request(self._c, "GET", f"{self._base}/currencies", timeout_s=self._read)
        return ProviderCapabilities(currencies=frozenset(data.get("currencies", [])))

    async def health(self) -> bool:
        data = await request(self._c, "GET", f"{self._base}/status", timeout_s=self._read)
        return bool(data) and data.get("message") == "OK"
