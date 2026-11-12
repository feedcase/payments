import logging
import time
from collections.abc import Callable

from redis.asyncio import Redis

from modules.payments.config import BreakerSettings
from modules.payments.domain.value_objects import ProviderCode

log = logging.getLogger(__name__)


class RedisCircuitBreaker:
    """Общий для всех инстансов breaker (§2.6).

    CLOSED: скользящее окно бакетов ok/fail. OPEN: ключ `open` с TTL open_s.
    HALF_OPEN: `open` истёк, но жив `half` — пропускаем один пробный вызов раз в
    probe_interval_s (SET NX). Успех пробы закрывает breaker, провал — снова открывает.
    Любая ошибка Redis — breaker «закрыт»: лучше вызвать провайдера, чем отказать всем.
    """

    def __init__(
        self,
        redis: Redis,
        settings: BreakerSettings,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._r = redis
        self._s = settings
        self._now = now

    def _k(self, code: str, part: str) -> str:
        return f"pay:cb:{code}:{part}"

    def _buckets(self, code: str) -> list[tuple[str, str]]:
        cur = int(self._now() // self._s.bucket_s)
        n = max(1, self._s.window_s // self._s.bucket_s)
        return [
            (self._k(code, f"ok:{b}"), self._k(code, f"fail:{b}"))
            for b in range(cur - n + 1, cur + 1)
        ]

    async def is_available(self, code: ProviderCode) -> bool:
        try:
            is_open, half = (
                await self._r.exists(self._k(code, "open")),
                await self._r.exists(self._k(code, "half")),
            )
            if is_open:
                return False
            if half:
                return bool(
                    await self._r.set(
                        self._k(code, "probe"), "1", nx=True, ex=self._s.probe_interval_s
                    )
                )
            return True
        except Exception:
            log.warning("breaker unavailable, treating %s as closed", code)
            return True

    async def record(self, code: ProviderCode, ok: bool) -> None:
        try:
            if await self._r.exists(self._k(code, "half")) and not await self._r.exists(
                self._k(code, "open")
            ):
                if ok:
                    await self._close(code)
                else:
                    await self._open(code)
                return
            ok_key, fail_key = self._buckets(code)[-1]
            key = ok_key if ok else fail_key
            async with self._r.pipeline(transaction=False) as p:
                p.incr(key)
                p.expire(key, self._s.window_s + self._s.bucket_s)
                await p.execute()
            if ok:
                return
            keys = [k for pair in self._buckets(code) for k in pair]
            values = await self._r.mget(keys)
            counts = [int(v or 0) for v in values]
            oks, fails = sum(counts[0::2]), sum(counts[1::2])
            total = oks + fails
            if total >= self._s.min_calls and fails / total >= self._s.failure_ratio:
                log.warning("breaker OPEN for %s (%s/%s failed)", code, fails, total)
                await self._open(code)
        except Exception:
            log.warning("breaker record failed for %s", code)

    async def _open(self, code: str) -> None:
        async with self._r.pipeline(transaction=True) as p:
            p.set(self._k(code, "open"), "1", ex=self._s.open_s)
            p.set(self._k(code, "half"), "1", ex=self._s.open_s * 4)
            p.delete(self._k(code, "probe"))
            p.delete(*[k for pair in self._buckets(code) for k in pair])
            await p.execute()

    async def _close(self, code: str) -> None:
        log.info("breaker CLOSED for %s", code)
        async with self._r.pipeline(transaction=True) as p:
            p.delete(self._k(code, "half"), self._k(code, "probe"))
            p.delete(*[k for pair in self._buckets(code) for k in pair])
            await p.execute()
