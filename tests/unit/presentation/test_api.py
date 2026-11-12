from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import jwt
import pytest

from app.main import create_app
from app.settings import JwtSettings
from modules.payments.application.use_cases import CreatePayment, GetPayment, Reinvoice
from modules.payments.domain.errors import FailureClass
from modules.payments.presentation.deps import PaymentsFacade
from tests.fakes import (
    Env,
    FakeDispatcher,
    FakeLock,
    FakeProvider,
    FakeRateLimiter,
    failure,
)

SECRET = "test-secret-at-least-32-bytes-long!!"
USER = uuid4()


def token(user: UUID = USER) -> str:
    return jwt.encode({"sub": str(user)}, SECRET, algorithm="HS256")


class Harness:
    def __init__(self, *providers, allow=True):
        self.env = Env(*(providers or (FakeProvider("p1"),)))
        self.enqueued: list[UUID] = []
        self.dispatcher = FakeDispatcher()
        self.limiter = FakeRateLimiter(allow)

        async def enqueue(eid: UUID) -> None:
            self.enqueued.append(eid)

        facade = PaymentsFacade(
            create=CreatePayment(
                uow_factory=self.env.uow,
                cascade=self.env.cascade,
                dispatcher=self.dispatcher,
                lock=FakeLock(),
                rate_limiter=self.limiter,
                clock=self.env.clock,
            ),
            get=GetPayment(uow_factory=self.env.uow),
            reinvoice=Reinvoice(
                uow_factory=self.env.uow,
                cascade=self.env.cascade,
                dispatcher=self.dispatcher,
                clock=self.env.clock,
            ),
            registry=self.env.registry,
            uow_factory=self.env.uow,
            enqueue_provider_event=enqueue,
        )
        container = SimpleNamespace(facade=facade, jwt=JwtSettings(secret=SECRET))
        self.app = create_app(container)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://t")


BODY = {
    "price": {"amount": "9.99", "currency": "USD"},
    "pay_currency": "usdttrc20",
    "method": "CRYPTO",
    "purpose": {"domain": "x", "ref": "1"},
}


def headers(key="k1", user=USER):
    return {"Authorization": f"Bearer {token(user)}", "Idempotency-Key": key}


async def test_create_201():
    h = Harness()
    async with h.client() as c:
        r = await c.post("/api/v1/payments", json=BODY, headers=headers())
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["status"] == "AWAITING_CUSTOMER"
    assert data["attempt"]["provider"] == "p1"
    assert data["attempt"]["checkout"]["pay_address"]
    assert data["price"] == {"amount": "9.99", "currency": "usd"}


async def test_create_202_deferred():
    p1 = FakeProvider("p1")
    h = Harness(p1, FakeProvider("p2"))

    async def slow(cmd):
        h.env.clock.advance(13)
        raise failure(FailureClass.PROVIDER_ERROR)

    p1.create = slow
    async with h.client() as c:
        r = await c.post("/api/v1/payments", json=BODY, headers=headers())
    assert r.status_code == 202 and r.json()["status"] == "ROUTING"


async def test_idem_replay_200():
    h = Harness()
    async with h.client() as c:
        r1 = await c.post("/api/v1/payments", json=BODY, headers=headers())
        r2 = await c.post("/api/v1/payments", json=BODY, headers=headers())
    assert r2.status_code == 200 and r2.json()["payment_id"] == r1.json()["payment_id"]


async def test_idem_key_body_mismatch_422():
    h = Harness()
    async with h.client() as c:
        await c.post("/api/v1/payments", json=BODY, headers=headers())
        other = BODY | {"price": {"amount": "10.00", "currency": "USD"}}
        r = await c.post("/api/v1/payments", json=other, headers=headers())
    assert r.status_code == 422 and r.json()["error"] == "idempotency_key_reused"


async def test_float_amount_rejected_422():
    h = Harness()
    async with h.client() as c:
        body = BODY | {"price": {"amount": 9.99, "currency": "USD"}}
        r = await c.post("/api/v1/payments", json=body, headers=headers())
    assert r.status_code == 422


async def test_missing_idem_key_400():
    h = Harness()
    async with h.client() as c:
        r = await c.post(
            "/api/v1/payments", json=BODY, headers={"Authorization": f"Bearer {token()}"}
        )
    assert r.status_code == 400


async def test_no_jwt_401():
    h = Harness()
    async with h.client() as c:
        r1 = await c.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})
        r2 = await c.post(
            "/api/v1/payments",
            json=BODY,
            headers={"Idempotency-Key": "k", "Authorization": "Bearer garbage"},
        )
    assert r1.status_code == 401 and r2.status_code == 401


async def test_rate_limited_429():
    h = Harness(allow=False)
    async with h.client() as c:
        r = await c.post("/api/v1/payments", json=BODY, headers=headers())
    assert r.status_code == 429


async def test_get_own_and_foreign():
    h = Harness()
    async with h.client() as c:
        pid = (await c.post("/api/v1/payments", json=BODY, headers=headers())).json()["payment_id"]
        own = await c.get(f"/api/v1/payments/{pid}", headers=headers())
        foreign = await c.get(f"/api/v1/payments/{pid}", headers=headers(user=uuid4()))
    assert own.status_code == 200 and foreign.status_code == 404


async def test_reinvoice_409_when_not_expired():
    h = Harness()
    async with h.client() as c:
        pid = (await c.post("/api/v1/payments", json=BODY, headers=headers())).json()["payment_id"]
        r = await c.post(f"/api/v1/payments/{pid}/reinvoice", headers=headers())
    assert r.status_code == 409


async def test_webhook_unknown_provider_404():
    h = Harness()
    async with h.client() as c:
        r = await c.post("/webhooks/nope", content=b"x")
    assert r.status_code == 404


async def test_webhook_bad_sig_401():
    h = Harness()
    async with h.client() as c:
        r = await c.post("/webhooks/p1", content=b"p1-1", headers={"x-sig": "bad"})
    assert r.status_code == 401 and h.enqueued == []


async def test_webhook_duplicate_not_enqueued():
    h = Harness()
    async with h.client() as c:
        r1 = await c.post("/webhooks/p1", content=b"p1-1", headers={"x-sig": "ok"})
        r2 = await c.post("/webhooks/p1", content=b"p1-1", headers={"x-sig": "ok"})
    assert r1.status_code == 200 and r2.status_code == 200
    assert len(h.enqueued) == 1


async def test_health_and_metrics():
    h = Harness()
    async with h.client() as c:
        assert (await c.get("/health")).status_code == 200
        m = await c.get("/metrics")
    assert m.status_code == 200


@pytest.mark.parametrize("bad", ["", "a" * 200])
async def test_idem_key_length_validated(bad):
    h = Harness()
    async with h.client() as c:
        r = await c.post("/api/v1/payments", json=BODY, headers=headers(key=bad))
    assert r.status_code == 400


async def test_webhook_enqueue_failure_still_200():
    """Review I3: событие уже в inbox — его подберёт RecoverStuck, провайдеру отвечаем 200."""
    h = Harness()

    async def broken(eid):
        raise ConnectionError("broker down")

    h.app.state.container.facade = h.app.state.container.facade.__class__(
        **{**h.app.state.container.facade.__dict__, "enqueue_provider_event": broken}
    )
    async with h.client() as c:
        r = await c.post("/webhooks/p1", content=b"p1-1", headers={"x-sig": "ok"})
    assert r.status_code == 200
    assert len(h.env.store.inbox) == 1
