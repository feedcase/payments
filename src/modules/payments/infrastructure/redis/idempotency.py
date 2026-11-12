import time
from uuid import UUID

from redis.asyncio import Redis


class RedisIdempotencyLock:
    def __init__(self, redis: Redis, ttl_s: int = 60) -> None:
        self._r = redis
        self._ttl = ttl_s

    @staticmethod
    def _key(user_id: UUID, key: str) -> str:
        return f"pay:idem:{user_id}:{key}"

    async def acquire(self, user_id: UUID, key: str) -> bool:
        return bool(await self._r.set(self._key(user_id, key), "1", nx=True, ex=self._ttl))

    async def release(self, user_id: UUID, key: str) -> None:
        await self._r.delete(self._key(user_id, key))


class RedisRateLimiter:
    """Фиксированное окно в минуту: `INCR pay:rl:{user}:{minute}` + `EXPIRE 60`."""

    def __init__(self, redis: Redis, per_min: int) -> None:
        self._r = redis
        self._per_min = per_min

    async def allow(self, user_id: UUID) -> bool:
        key = f"pay:rl:{user_id}:{int(time.time() // 60)}"
        async with self._r.pipeline(transaction=True) as p:
            p.incr(key)
            p.expire(key, 60)
            count, _ = await p.execute()
        return int(count) <= self._per_min
