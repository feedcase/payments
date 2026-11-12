from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from modules.payments.config import (
    BreakerSettings,
    CascadeSettings,
    CoinGateSettings,
    MockSettings,
    NowPaymentsSettings,
)


class DbSettings(BaseModel):
    url: str
    echo: bool = False


class RedisSettings(BaseModel):
    url: str


class JwtSettings(BaseModel):
    secret: str = "dev-secret-change-me-32-bytes-minimum"
    algorithm: str = "HS256"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__", env_file=".env", extra="ignore")

    env: str = "dev"
    db: DbSettings
    redis: RedisSettings
    jwt: JwtSettings = JwtSettings()
    cascade: CascadeSettings = CascadeSettings()
    breaker: BreakerSettings = BreakerSettings()
    nowpayments: NowPaymentsSettings = NowPaymentsSettings()
    coingate: CoinGateSettings = CoinGateSettings()
    mocks: list[MockSettings] = []
    public_base_url: str = "http://localhost:8000"
    rate_limit_per_min: int = 10
    # inline — фоновые шаги в asyncio-задачах процесса API (dev/тесты); taskiq — воркеры
    task_queue: Literal["inline", "taskiq"] = "inline"

    @model_validator(mode="after")
    def _no_inline_outside_dev(self) -> "Settings":
        # inline: нет расписания (сверка, sweeper) и задачи теряются при рестарте
        if self.task_queue == "inline" and self.env not in ("dev", "test"):
            raise ValueError("TASK_QUEUE=inline is only allowed for ENV=dev|test")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
