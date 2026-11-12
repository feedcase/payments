"""In-memory реализации портов для unit-тестов application/presentation."""

import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from modules.payments.application.cascade import CascadeOrchestrator
from modules.payments.application.ports import (
    CreateProviderPayment,
    InboxEvent,
    InvalidWebhook,
    OutboxRecord,
    ProviderCapabilities,
    ProviderCreateResult,
    ProviderEventRef,
    ProviderFailure,
    ProviderPaymentState,
)
from modules.payments.application.routing import Router
from modules.payments.config import CascadeSettings
from modules.payments.domain.entities import Payment
from modules.payments.domain.errors import AttemptFailure, FailureClass
from modules.payments.domain.events import DomainEvent
from modules.payments.domain.statuses import ACTIVE_ATTEMPT, AttemptStatus
from modules.payments.domain.value_objects import (
    Checkout,
    Money,
    PaymentMethod,
    ProviderCode,
    Purpose,
)

T0 = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self._now = T0
        self._mono = 1000.0

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)
        self._mono += seconds


def failure(cls: FailureClass, code: str = "x") -> ProviderFailure:
    return ProviderFailure(AttemptFailure(cls=cls, code=code))


class FakeProvider:
    """create() отдаёт по очереди элементы `script`: "ok" | ProviderFailure | ("sleep", s)."""

    def __init__(self, code: str, script: Sequence[Any] = ("ok",), clock: FakeClock | None = None):
        self.code = ProviderCode(code)
        self.script = list(script)
        self.clock = clock
        self.created: list[CreateProviderPayment] = []
        self.states: dict[str, ProviderPaymentState | None] = {}
        self.fetch_calls = 0
        self.hold: asyncio.Event | None = None
        self.fixed_pid: str | None = None

    async def create(self, cmd: CreateProviderPayment) -> ProviderCreateResult:
        self.created.append(cmd)
        if self.hold is not None:
            await self.hold.wait()
        step = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(step, tuple) and step[0] == "sleep":
            assert self.clock is not None
            self.clock.advance(step[1])
            step = "ok"
        if isinstance(step, ProviderFailure):
            raise step
        pid = self.fixed_pid or f"{self.code}-{len(self.created)}"
        self.states[pid] = ProviderPaymentState(status=AttemptStatus.CREATED)
        return ProviderCreateResult(
            provider_payment_id=pid,
            checkout=Checkout(pay_address=f"addr-{pid}", expires_at=T0 + timedelta(minutes=30)),
        )

    async def fetch(self, provider_payment_id: str) -> ProviderPaymentState | None:
        self.fetch_calls += 1
        return self.states.get(provider_payment_id)

    def parse_webhook(self, headers: Mapping[str, str], body: bytes) -> ProviderEventRef:
        if headers.get("x-sig") != "ok":
            raise InvalidWebhook("bad sig")
        pid = body.decode()
        return ProviderEventRef(provider_payment_id=pid, order_id=None)

    async def capabilities(self, pay_currencies: Sequence[str]) -> ProviderCapabilities:
        return ProviderCapabilities(currencies=frozenset(pay_currencies))

    async def health(self) -> bool:
        return True


class FakeRegistry:
    def __init__(self, *providers: FakeProvider) -> None:
        self._p = {p.code: p for p in providers}

    def get(self, code: ProviderCode) -> FakeProvider:
        return self._p[code]

    def has(self, code: ProviderCode) -> bool:
        return code in self._p

    def all(self) -> list[FakeProvider]:
        return list(self._p.values())


class FakeBreaker:
    def __init__(self) -> None:
        self.open: set[str] = set()
        self.records: list[tuple[str, bool]] = []

    async def is_available(self, code: ProviderCode) -> bool:
        return code not in self.open

    async def record(self, code: ProviderCode, ok: bool) -> None:
        self.records.append((code, ok))


class FakeCaps:
    def __init__(self) -> None:
        self.data: dict[str, ProviderCapabilities] = {}

    async def get(self, code: ProviderCode) -> ProviderCapabilities | None:
        return self.data.get(code)

    async def set(self, code: ProviderCode, caps: ProviderCapabilities) -> None:
        self.data[code] = caps


class FakeLock:
    def __init__(self, broken: bool = False) -> None:
        self.held: set[tuple[UUID, str]] = set()
        self.broken = broken

    async def acquire(self, user_id: UUID, key: str) -> bool:
        if self.broken:
            raise ConnectionError("redis down")
        if (user_id, key) in self.held:
            return False
        self.held.add((user_id, key))
        return True

    async def release(self, user_id: UUID, key: str) -> None:
        if self.broken:
            raise ConnectionError("redis down")
        self.held.discard((user_id, key))


class FakeRateLimiter:
    def __init__(self, allow: bool = True) -> None:
        self._allow = allow

    async def allow(self, user_id: UUID) -> bool:
        return self._allow


class FakeDispatcher:
    def __init__(self) -> None:
        self.deferred: list[UUID] = []

    async def continue_later(self, payment_id: UUID) -> None:
        self.deferred.append(payment_id)


class FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, event_type: str, payload: dict[str, Any]) -> None:
        self.published.append((event_type, payload))


# ---- in-memory storage -------------------------------------------------------------------


class Store:
    def __init__(self, routes: dict[str, list[str]] | None = None) -> None:
        self.payments: dict[UUID, Payment] = {}
        self.routes = routes or {"*": ["p1", "p2"]}
        self.inbox: dict[UUID, dict[str, Any]] = {}
        self.inbox_hashes: set[tuple[str, str]] = set()
        self.outbox: list[dict[str, Any]] = []
        self.commits = 0


class _Payments:
    def __init__(self, store: Store, uow: "InMemoryUoW") -> None:
        self.s = store
        self.uow = uow

    async def get(self, payment_id: UUID) -> Payment | None:
        p = self.s.payments.get(payment_id)
        return p.model_copy(deep=True) if p else None

    get_for_update = get

    async def find_by_idempotency(self, user_id: UUID, key: str) -> Payment | None:
        for p in self.s.payments.values():
            if p.user_id == user_id and p.idempotency_key == key:
                return p.model_copy(deep=True)
        return None

    async def add(self, payment: Payment) -> None:
        from modules.payments.application.errors import DuplicatePayment

        if await self.find_by_idempotency(payment.user_id, payment.idempotency_key):
            raise DuplicatePayment()
        self.uow.pending.append(payment)

    async def save(self, payment: Payment) -> None:
        self.uow.pending.append(payment)

    async def find_attempt(self, provider, provider_payment_id=None, attempt_id=None):
        for p in self.s.payments.values():
            for a in p.attempts:
                if a.provider != provider:
                    continue
                if (provider_payment_id and a.provider_payment_id == provider_payment_id) or (
                    attempt_id and a.id == attempt_id
                ):
                    return p.id, a.id
        return None

    async def attempts_to_reconcile(self, now, active_after_s, closed_after_s, limit):
        closed = {AttemptStatus.SUPERSEDED, AttemptStatus.EXPIRED, AttemptStatus.CREATE_UNKNOWN}
        active = ACTIVE_ATTEMPT - {AttemptStatus.CREATING}
        out = []
        for p in self.s.payments.values():
            for a in p.attempts:
                age = (now - (a.checked_at or a.updated_at)).total_seconds()
                if not a.provider_payment_id:
                    continue
                if (a.status in active and age >= active_after_s) or (
                    a.status in closed and age >= closed_after_s
                ):
                    out.append((p.id, a.id))
        return out[:limit]

    async def stuck_payments(self, now, older_than_s, limit):
        from modules.payments.domain.statuses import PaymentStatus

        return [
            p.id
            for p in self.s.payments.values()
            if p.status in (PaymentStatus.NEW, PaymentStatus.ROUTING)
            and (now - p.updated_at).total_seconds() >= older_than_s
        ][:limit]

    async def attempts_past_expiry(self, now, grace_min, limit):
        out = []
        for p in self.s.payments.values():
            for a in p.attempts:
                if (
                    a.status in (AttemptStatus.CREATED, AttemptStatus.CONFIRMING)
                    and a.checkout
                    and a.checkout.expires_at + timedelta(minutes=grace_min) < now
                ):
                    out.append((p.id, a.id))
        return out[:limit]


class _Routes:
    def __init__(self, store: Store) -> None:
        self.s = store

    async def get(self, method: PaymentMethod, pay_currency: str) -> list[ProviderCode]:
        codes = self.s.routes.get(pay_currency) or self.s.routes.get("*") or []
        return [ProviderCode(c) for c in codes]

    async def currencies(self) -> list[str]:
        return [c for c in self.s.routes if c != "*"] or ["usdttrc20"]


class _Inbox:
    def __init__(self, store: Store) -> None:
        self.s = store

    async def add(self, provider, event_hash, ref: ProviderEventRef, raw: str) -> UUID | None:
        if (provider, event_hash) in self.s.inbox_hashes:
            return None
        self.s.inbox_hashes.add((provider, event_hash))
        eid = uuid4()
        self.s.inbox[eid] = {
            "provider": provider,
            "ref": ref,
            "processed_at": None,
            "note": None,
        }
        return eid

    async def get(self, event_id: UUID) -> InboxEvent | None:
        e = self.s.inbox.get(event_id)
        if e is None:
            return None
        return InboxEvent(
            id=event_id,
            provider=e["provider"],
            provider_payment_id=e["ref"].provider_payment_id,
            order_id=e["ref"].order_id,
            processed_at=e["processed_at"],
        )

    async def mark_processed(self, event_id: UUID, note: str | None = None) -> None:
        self.s.inbox[event_id]["processed_at"] = T0
        self.s.inbox[event_id]["note"] = note

    async def unprocessed(self, now, older_than_s, limit):
        return [
            eid
            for eid, e in self.s.inbox.items()
            if e["processed_at"] is None and (now - T0).total_seconds() >= older_than_s
        ][:limit]


class _Outbox:
    def __init__(self, store: Store) -> None:
        self.s = store

    async def add(self, event: DomainEvent) -> None:
        self.s.outbox.append(
            {
                "id": event.event_id,
                "event_type": event.event_type,
                "payload": event.model_dump(mode="json"),
                "published": False,
            }
        )

    async def fetch_unpublished(self, batch: int) -> list[OutboxRecord]:
        return [
            OutboxRecord(id=r["id"], event_type=r["event_type"], payload=r["payload"])
            for r in self.s.outbox
            if not r["published"]
        ][:batch]

    async def mark_published(self, ids) -> None:
        for r in self.s.outbox:
            if r["id"] in ids:
                r["published"] = True


class InMemoryUoW:
    def __init__(self, store: Store) -> None:
        self.s = store
        self.pending: list[Payment] = []
        self.payments = _Payments(store, self)
        self.routes = _Routes(store)
        self.events = _Inbox(store)
        self.outbox = _Outbox(store)

    async def __aenter__(self) -> "InMemoryUoW":
        self.pending = []
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.pending = []

    async def commit(self) -> None:
        from modules.payments.application.errors import DuplicateProviderPaymentId

        for p in self.pending:
            for a in p.attempts:
                for other in self.s.payments.values():
                    if other.id != p.id and any(
                        o.provider == a.provider
                        and a.provider_payment_id
                        and o.provider_payment_id == a.provider_payment_id
                        for o in other.attempts
                    ):
                        raise DuplicateProviderPaymentId(a.provider_payment_id)
        for p in self.pending:
            for ev in p.pull_events():
                await self.outbox.add(ev)
            p.version += 1
            self.s.payments[p.id] = p.model_copy(deep=True)
        self.pending = []
        self.s.commits += 1

    def events_of(self, payment_id: UUID) -> list[str]:
        return [
            r["event_type"] for r in self.s.outbox if r["payload"]["payment_id"] == str(payment_id)
        ]


class Env:
    """Каскад на in-memory портах: одна точка сборки для тестов application/presentation."""

    def __init__(self, *providers: FakeProvider, routes=None):
        self.clock = FakeClock()
        for p in providers:
            p.clock = self.clock
        self.store = Store(routes)
        self.registry = FakeRegistry(*providers)
        self.breaker = FakeBreaker()
        self.caps = FakeCaps()
        self.settings = CascadeSettings()
        self.router = Router(self.registry, self.caps, self.breaker)
        self.cascade = CascadeOrchestrator(
            uow_factory=self.uow,
            registry=self.registry,
            router=self.router,
            breaker=self.breaker,
            clock=self.clock,
            settings=self.settings,
            callback_url_for=lambda code: f"http://cb/{code}",
        )

    def uow(self) -> InMemoryUoW:
        return InMemoryUoW(self.store)

    async def new_payment(self, amount="9.99", currency="usdttrc20") -> Payment:
        p = Payment.create(
            user_id=uuid4(),
            price=Money(amount=amount, currency="USD"),
            pay_currency=currency,
            purpose=Purpose(domain="x", ref="1"),
            idempotency_key=str(uuid4()),
            request_hash="h",
            now=self.clock.now(),
        )
        async with self.uow() as uow:
            await uow.payments.add(p)
            await uow.commit()
        return p

    def payment(self, pid) -> Payment:
        return self.store.payments[pid]
