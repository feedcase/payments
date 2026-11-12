import logging

from redis.asyncio import Redis

from modules.payments.application.ports import ProviderCapabilities
from modules.payments.domain.value_objects import ProviderCode

log = logging.getLogger(__name__)


class RedisCapabilityCache:
    def __init__(self, redis: Redis, ttl_s: int = 900) -> None:
        self._r = redis
        self._ttl = ttl_s

    @staticmethod
    def _key(code: str) -> str:
        return f"pay:cap:{code}"

    async def get(self, code: ProviderCode) -> ProviderCapabilities | None:
        try:
            raw = await self._r.get(self._key(code))
        except Exception:
            log.warning("capability cache unavailable")
            return None
        return ProviderCapabilities.model_validate_json(raw) if raw else None

    async def set(self, code: ProviderCode, caps: ProviderCapabilities) -> None:
        await self._r.set(self._key(code), caps.model_dump_json(), ex=self._ttl)
