import json
from typing import Any

from redis.asyncio import Redis


class RedisStreamPublisher:
    """Публикация доменных событий в Redis Stream; подписчики читают через XREADGROUP."""

    def __init__(self, redis: Redis, stream: str = "payments.events", maxlen: int = 100_000):
        self._r = redis
        self._stream = stream
        self._maxlen = maxlen

    async def publish(self, event_type: str, payload: dict[str, Any]) -> None:
        await self._r.xadd(
            self._stream,
            {"type": event_type, "payload": json.dumps(payload, sort_keys=True)},
            maxlen=self._maxlen,
            approximate=True,
        )
