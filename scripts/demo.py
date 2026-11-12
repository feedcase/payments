"""Демо сценариев каскада D1–D8 (docs/01-providers.md §1.6) против docker compose.

    docker compose up -d --build
    poetry run python scripts/demo.py

Без sandbox-ключей роли провайдеров играют два экземпляра mock: mock_primary и mock_backup.
"""

import asyncio
import json
import os
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
import httpx
import jwt
import redis.asyncio as aioredis

API = os.environ.get("DEMO_API", "http://localhost:8000")
MOCK = os.environ.get("DEMO_MOCK", "http://localhost:9000")
PG = os.environ.get("DEMO_PG", "postgresql://payments:payments@localhost:5433/payments")
REDIS = os.environ.get("DEMO_REDIS", "redis://localhost:6390/0")
SECRET = os.environ.get("JWT__SECRET", "dev-secret-change-me-32-bytes-minimum")
PRIMARY, BACKUP = "mock_primary", "mock_backup"
USER = str(uuid.uuid4())
TOKEN = jwt.encode({"sub": USER}, SECRET, algorithm="HS256")
BODY = {
    "price": {"amount": "9.99", "currency": "USD"},
    "pay_currency": "usdttrc20",
    "purpose": {"domain": "demo", "ref": "order-1"},
}


class Failed(AssertionError):
    pass


def check(cond: bool, what: str) -> None:
    if not cond:
        raise Failed(what)


class Demo:
    def __init__(self, api: httpx.AsyncClient, mock: httpx.AsyncClient, r: aioredis.Redis):
        self.api, self.mock, self.r = api, mock, r

    async def scenario(self, instance: str, name: str) -> None:
        resp = await self.mock.post(f"/{instance}/_control/scenario", json={"scenario": name})
        resp.raise_for_status()

    async def create(self) -> httpx.Response:
        return await self.api.post(
            "/api/v1/payments",
            json=BODY,
            headers={"Authorization": f"Bearer {TOKEN}", "Idempotency-Key": str(uuid.uuid4())},
        )

    async def get(self, pid: str) -> dict[str, Any]:
        resp = await self.api.get(
            f"/api/v1/payments/{pid}", headers={"Authorization": f"Bearer {TOKEN}"}
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data

    async def reinvoice(self, pid: str) -> httpx.Response:
        return await self.api.post(
            f"/api/v1/payments/{pid}/reinvoice", headers={"Authorization": f"Bearer {TOKEN}"}
        )

    async def wait_status(self, pid: str, status: str, timeout: float = 20) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            data = await self.get(pid)
            if data["status"] == status:
                return data
            if time.monotonic() > deadline:
                raise Failed(f"status {data['status']} != {status} after {timeout}s")
            await asyncio.sleep(0.5)

    async def provider_payment(self, instance: str, attempt_id: str) -> dict[str, Any]:
        items = (await self.mock.get(f"/{instance}/_control/payments")).json()
        return next(p for p in items if p["order_id"] == attempt_id)

    async def pay(self, instance: str, provider_payment_id: str, status: str) -> None:
        resp = await self.mock.post(
            f"/{instance}/_control/pay/{provider_payment_id}", params={"status": status}
        )
        resp.raise_for_status()

    async def events_for(self, pid: str, timeout: float = 20) -> list[str]:
        deadline = time.monotonic() + timeout
        while True:
            entries = await self.r.xrange("payments.events")
            types = [
                e[1]["type"] for e in entries if json.loads(e[1]["payload"])["payment_id"] == pid
            ]
            if types or time.monotonic() > deadline:
                return types
            await asyncio.sleep(0.5)

    # ---- сценарии ----------------------------------------------------------------------

    async def d1_success_first_provider(self) -> str:
        await self.scenario(PRIMARY, "ok")
        r = await self.create()
        check(r.status_code == 201, f"HTTP {r.status_code}")
        d = r.json()
        check(d["attempt"]["provider"] == PRIMARY, "счёт не у основного провайдера")
        d = await self.wait_status(d["payment_id"], "SUCCEEDED")
        return f"{PRIMARY}, глубина 1, вебхуки → SUCCEEDED, received={d['received_total']}"

    async def d2_primary_down(self) -> str:
        await self.scenario(PRIMARY, "http_500")
        await self.scenario(BACKUP, "manual")
        r = await self.create()
        d = r.json()
        check(r.status_code == 201, f"HTTP {r.status_code}")
        check(d["attempt"]["provider"] == BACKUP and d["attempt"]["seq"] == 2, str(d["attempt"]))
        return f"{PRIMARY}: 500 → счёт у {BACKUP} (seq 2) в том же запросе"

    async def d3_unsupported(self) -> str:
        await self.scenario(PRIMARY, "unsupported")
        await self.scenario(BACKUP, "manual")
        d = (await self.create()).json()
        check(d["attempt"]["provider"] == BACKUP, str(d["attempt"]))
        return f"{PRIMARY}: валюта не поддерживается (без штрафа breaker'у) → {BACKUP}"

    async def d4_customer_failed(self) -> str:
        await self.scenario(PRIMARY, "manual")
        await self.scenario(BACKUP, "manual")
        d = (await self.create()).json()
        pp = await self.provider_payment(PRIMARY, d["attempt"]["attempt_id"])
        await self.pay(PRIMARY, pp["id"], "failed")
        deadline = time.monotonic() + 20
        while True:
            cur = await self.get(d["payment_id"])
            if cur["attempt"] and cur["attempt"]["provider"] == BACKUP:
                break
            check(time.monotonic() < deadline, f"нет новой попытки: {cur}")
            await asyncio.sleep(0.5)
        check(cur["status"] == "AWAITING_CUSTOMER", cur["status"])
        return f"{PRIMARY}: failed (вебхук) → воркер выставил новый счёт у {BACKUP}"

    async def d5_partial(self) -> str:
        await self.scenario(PRIMARY, "manual")
        d = (await self.create()).json()
        pp = await self.provider_payment(PRIMARY, d["attempt"]["attempt_id"])
        await self.pay(PRIMARY, pp["id"], "partially_paid")
        cur = await self.wait_status(d["payment_id"], "PARTIALLY_PAID")
        check(cur["attempt"]["seq"] == 1, "каскад не должен запускаться при недоплате")
        return f"недоплата {cur['received_total']} → PARTIALLY_PAID, каскада нет"

    async def d6_late_payment_overpaid(self) -> str:
        await self.scenario(PRIMARY, "manual")
        await self.scenario(BACKUP, "manual")
        d = (await self.create()).json()
        pid = d["payment_id"]
        old = await self.provider_payment(PRIMARY, d["attempt"]["attempt_id"])
        await self.pay(PRIMARY, old["id"], "expired")
        await self.wait_status(pid, "EXPIRED")
        await self.scenario(PRIMARY, "http_500")
        r = await self.reinvoice(pid)
        d2 = r.json()
        check(d2["attempt"]["provider"] == BACKUP, str(d2["attempt"]))
        new = await self.provider_payment(BACKUP, d2["attempt"]["attempt_id"])
        await self.pay(BACKUP, new["id"], "finished")
        await self.wait_status(pid, "SUCCEEDED")
        await self.pay(PRIMARY, old["id"], "finished")  # покупатель оплатил старый счёт
        deadline = time.monotonic() + 20
        while "PaymentOverpaid" not in (types := await self.events_for(pid)):
            check(time.monotonic() < deadline, f"нет PaymentOverpaid: {types}")
            await asyncio.sleep(0.5)
        cur = await self.get(pid)
        return f"истёк → перевыставлен у {BACKUP} → оплачен → старый счёт оплачен: {types}, received={cur['received_total']}"

    async def d7_all_down(self) -> str:
        await self.scenario(PRIMARY, "http_500")
        await self.scenario(BACKUP, "http_500")
        d = (await self.create()).json()
        check(d["status"] == "FAILED", d["status"])
        check(d["failure_reason"] == "no_provider_available", str(d["failure_reason"]))
        return "оба провайдера 500 → FAILED: no_provider_available"

    async def d8_ghost_harmless(self) -> str:
        await self.scenario(PRIMARY, "ghost_success")
        await self.scenario(BACKUP, "manual")
        d = (await self.create()).json()
        check(d["attempt"]["provider"] == BACKUP, str(d["attempt"]))
        ghosts = (await self.mock.get(f"/{PRIMARY}/_control/payments")).json()
        check(any(g["status"] == "waiting" for g in ghosts), "нет призрачного счёта")
        return f"{PRIMARY} создал счёт, но ответил 500 → его адрес не показан, счёт у {BACKUP}"


async def setup_routes() -> None:
    conn = await asyncpg.connect(PG)
    try:
        await conn.execute(
            "INSERT INTO cascade_routes(method, pay_currency, providers) "
            "VALUES ('CRYPTO', 'usdttrc20', $1) "
            "ON CONFLICT (method, pay_currency) DO UPDATE SET providers = EXCLUDED.providers",
            [PRIMARY, BACKUP],
        )
    finally:
        await conn.close()


async def main() -> int:
    await setup_routes()
    async with (
        httpx.AsyncClient(base_url=API, timeout=30) as api,
        httpx.AsyncClient(base_url=MOCK, timeout=10) as mock,
    ):
        r = aioredis.from_url(REDIS, decode_responses=True)
        demo = Demo(api, mock, r)
        scenarios: list[tuple[str, Callable[[], Awaitable[str]]]] = [
            ("D1", demo.d1_success_first_provider),
            ("D2", demo.d2_primary_down),
            ("D3", demo.d3_unsupported),
            ("D4", demo.d4_customer_failed),
            ("D5", demo.d5_partial),
            ("D6", demo.d6_late_payment_overpaid),
            ("D7", demo.d7_all_down),
            ("D8", demo.d8_ghost_harmless),
        ]
        failed = 0
        for name, fn in scenarios:
            try:
                print(f"{name} PASS  {await fn()}")
            except Exception as e:
                failed += 1
                print(f"{name} FAIL  {type(e).__name__}: {e}")
        await r.aclose()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
