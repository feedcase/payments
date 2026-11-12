from collections.abc import Sequence

import httpx

from modules.payments.application.ports import PaymentProviderPort
from modules.payments.config import (
    CascadeSettings,
    CoinGateSettings,
    MockSettings,
    NowPaymentsSettings,
)
from modules.payments.domain.value_objects import ProviderCode
from modules.payments.infrastructure.providers.coingate import CoinGateProvider
from modules.payments.infrastructure.providers.mock import MockProvider
from modules.payments.infrastructure.providers.nowpayments import NowPaymentsProvider


class StaticRegistry:
    def __init__(self, providers: Sequence[PaymentProviderPort]) -> None:
        self._p = {p.code: p for p in providers}

    def get(self, code: ProviderCode) -> PaymentProviderPort:
        return self._p[code]

    def has(self, code: ProviderCode) -> bool:
        return code in self._p

    def all(self) -> list[PaymentProviderPort]:
        return list(self._p.values())


def _client(
    cascade: CascadeSettings, transport: httpx.AsyncBaseTransport | None = None
) -> httpx.AsyncClient:
    """Отдельный пул на провайдера: медленный провайдер не забирает соединения у других."""
    return httpx.AsyncClient(
        transport=transport,
        timeout=httpx.Timeout(
            cascade.read_timeout_s,
            connect=cascade.connect_timeout_s,
            pool=cascade.connect_timeout_s,
        ),
        limits=httpx.Limits(
            max_connections=cascade.bulkhead_per_provider,
            max_keepalive_connections=cascade.bulkhead_per_provider,
        ),
    )


def build_registry(
    *,
    cascade: CascadeSettings,
    nowpayments: NowPaymentsSettings,
    coingate: CoinGateSettings,
    mocks: Sequence[MockSettings],
    transport: httpx.AsyncBaseTransport | None = None,
) -> tuple[StaticRegistry, list[httpx.AsyncClient]]:
    providers: list[PaymentProviderPort] = []
    clients: list[httpx.AsyncClient] = []
    if nowpayments.enabled:
        c = _client(cascade, transport)
        clients.append(c)
        providers.append(
            NowPaymentsProvider(nowpayments, c, cascade.invoice_ttl_min, cascade.read_timeout_s)
        )
    if coingate.enabled:
        c = _client(cascade, transport)
        clients.append(c)
        providers.append(
            CoinGateProvider(coingate, c, cascade.invoice_ttl_min, cascade.read_timeout_s)
        )
    for m in mocks:
        c = _client(cascade, transport)
        clients.append(c)
        providers.append(
            MockProvider(
                code=m.code,
                base_url=m.base_url,
                secret=m.secret,
                client=c,
                read_timeout_s=cascade.read_timeout_s,
            )
        )
    return StaticRegistry(providers), clients
