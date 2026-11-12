"""Настройки модуля payments (публичная часть модуля, без зависимостей от app)."""

from pydantic import BaseModel


class CascadeSettings(BaseModel):
    deadline_s: float = 15.0
    min_step_s: float = 3.0
    connect_timeout_s: float = 2.0
    read_timeout_s: float = 8.0
    invoice_ttl_min: int = 30
    reconcile_after_s: int = 120  # активные попытки (ждём денег)
    reconcile_closed_after_s: int = 900  # замещённые/истёкшие: только опоздавшие платежи
    reconcile_concurrency: int = 10
    stuck_after_s: int = 60  # NEW/ROUTING без движения дольше — шаг каскада потерян
    inbox_retry_after_s: int = 30  # вебхук сохранён, но не обработан (сбой очереди)
    expire_grace_min: int = 10
    bulkhead_per_provider: int = 50
    bulkhead_wait_s: float = 0.5


class BreakerSettings(BaseModel):
    window_s: int = 60
    bucket_s: int = 10
    failure_ratio: float = 0.5
    min_calls: int = 20
    open_s: int = 30
    probe_interval_s: int = 10


class NowPaymentsSettings(BaseModel):
    enabled: bool = False
    base_url: str = "https://api-sandbox.nowpayments.io/v1"
    api_key: str = ""
    ipn_secret: str = ""
    sandbox_case: str | None = None


class CoinGateSettings(BaseModel):
    enabled: bool = False
    base_url: str = "https://api-sandbox.coingate.com/v2"
    api_token: str = ""
    callback_secret: str = ""
    receive_currency: str = "DO_NOT_CONVERT"
    # наш код монеты (как у NOWPayments) → тикер CoinGate
    pay_currency_map: dict[str, str] = {"usdttrc20": "USDT", "btc": "BTC", "eth": "ETH"}


class MockSettings(BaseModel):
    code: str
    base_url: str
    secret: str
