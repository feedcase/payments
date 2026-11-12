"""Mock-провайдер для демонстрации каскада (docs/01-providers.md §1.5).

Отдельный сервис: каждое значение `{instance}` в пути — независимый «провайдер» со своим
сценарием, так один контейнер изображает и основного, и резервного провайдера.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import count
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger("mock_provider")

SCENARIOS = {
    "ok",
    "manual",
    "http_500",
    "timeout",
    "slow",
    "unsupported",
    "ghost_success",
    "webhook_dup",
    "webhook_reorder",
}
WEBHOOK_PLAN = {
    "ok": ["confirming", "finished"],
    "webhook_dup": ["finished", "finished"],
    "webhook_reorder": ["finished", "confirming"],
}


class ScenarioIn(BaseModel):
    scenario: str


class Instance:
    def __init__(self, name: str) -> None:
        self.name = name
        self.scenario = os.environ.get("MOCK_DEFAULT_SCENARIO", "ok")
        self.payments: dict[str, dict[str, Any]] = {}
        self._seq = count(1)
        # как у реальных провайдеров: id уникальны и после рестарта
        self._boot = uuid.uuid4().hex[:8]

    def new_id(self) -> str:
        return f"{self.name}-{self._boot}-{next(self._seq)}"


def create_app() -> FastAPI:
    app = FastAPI(title="mock payment provider")
    instances: dict[str, Instance] = {}
    tasks: set[asyncio.Task[None]] = set()
    secret = os.environ.get("MOCK_SECRET", "dev").encode()
    delay = float(os.environ.get("MOCK_WEBHOOK_DELAY_S", "1"))
    slow_s = float(os.environ.get("MOCK_SLOW_S", "6"))
    timeout_s = float(os.environ.get("MOCK_TIMEOUT_S", "30"))

    def inst(name: str) -> Instance:
        if name not in instances:
            instances[name] = Instance(name)
        return instances[name]

    async def send_webhook(payment: dict[str, Any]) -> None:
        body = json.dumps(
            {"id": payment["id"], "order_id": payment["order_id"], "status": payment["status"]}
        ).encode()
        sig = hmac.new(secret, body, hashlib.sha256).hexdigest()
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                await c.post(
                    payment["callback_url"],
                    content=body,
                    headers={"content-type": "application/json", "x-mock-sig": sig},
                )
        except httpx.HTTPError as e:
            log.warning("webhook to %s failed: %s", payment["callback_url"], e)

    async def play(payment: dict[str, Any], plan: list[str]) -> None:
        for status in plan:
            await asyncio.sleep(delay)
            set_status(payment, status)
            await send_webhook(payment)

    def set_status(payment: dict[str, Any], status: str) -> None:
        # статус у «провайдера» только вперёд: перестановку видит лишь потребитель вебхуков
        payment["status"] = status
        if status == "finished":
            payment["actually_paid"] = payment["pay_amount"]
        elif status == "partially_paid":
            payment["actually_paid"] = str(Decimal(payment["pay_amount"]) / 2)

    def spawn(coro: Any) -> None:
        t = asyncio.create_task(coro)
        tasks.add(t)
        t.add_done_callback(tasks.discard)

    @app.post("/{name}/_control/scenario")
    async def set_scenario(name: str, body: ScenarioIn) -> dict[str, str]:
        if body.scenario not in SCENARIOS:
            raise HTTPException(422, f"unknown scenario, use one of {sorted(SCENARIOS)}")
        inst(name).scenario = body.scenario
        return {"instance": name, "scenario": body.scenario}

    @app.get("/{name}/_control/payments")
    async def list_payments(name: str) -> list[dict[str, Any]]:
        return list(inst(name).payments.values())

    @app.post("/{name}/_control/pay/{pid}")
    async def pay(name: str, pid: str, status: str = "finished") -> dict[str, Any]:
        payment = inst(name).payments.get(pid)
        if payment is None:
            raise HTTPException(404)
        set_status(payment, status)
        await send_webhook(payment)
        return payment

    @app.post("/{name}/payments")
    async def create_payment(name: str, request: Request) -> JSONResponse:
        i = inst(name)
        req = await request.json()
        scenario = i.scenario
        if scenario == "http_500":
            return JSONResponse({"message": "internal error"}, status_code=500)
        if scenario == "unsupported":
            return JSONResponse({"code": "UNSUPPORTED", "message": "currency"}, status_code=400)
        if scenario == "timeout":
            await asyncio.sleep(timeout_s)
        if scenario == "slow":
            await asyncio.sleep(slow_s)
        pid = i.new_id()
        payment = {
            "id": pid,
            "order_id": req["order_id"],
            "status": "waiting",
            "pay_address": f"T{hashlib.sha1(pid.encode()).hexdigest()[:33]}",
            "pay_amount": str(req["price_amount"]),
            "pay_currency": req["pay_currency"],
            "actually_paid": None,
            "callback_url": req["callback_url"],
            "expires_at": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
        }
        i.payments[pid] = payment
        if scenario == "ghost_success":
            return JSONResponse({"message": "created, but we pretend it failed"}, status_code=500)
        if scenario in WEBHOOK_PLAN:
            spawn(play(payment, WEBHOOK_PLAN[scenario]))
        return JSONResponse(payment, status_code=201)

    @app.get("/{name}/payments/{pid}")
    async def get_payment(name: str, pid: str) -> dict[str, Any]:
        payment = inst(name).payments.get(pid)
        if payment is None:
            raise HTTPException(404)
        return payment

    @app.get("/{name}/currencies")
    async def currencies(name: str) -> dict[str, list[str]]:
        return {"currencies": ["usdttrc20", "btc", "eth"]}

    @app.get("/{name}/status")
    async def status(name: str) -> dict[str, str]:
        return {"message": "OK"}

    return app


app = create_app()
