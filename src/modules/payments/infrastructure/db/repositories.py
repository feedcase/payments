from collections.abc import Sequence
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import and_, case, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from modules.payments.application.errors import (
    ConcurrencyConflict,
    DuplicatePayment,
    DuplicateProviderPaymentId,
)
from modules.payments.application.ports import InboxEvent, OutboxRecord, ProviderEventRef
from modules.payments.domain.entities import Payment
from modules.payments.domain.events import DomainEvent
from modules.payments.domain.statuses import AttemptStatus, PaymentStatus
from modules.payments.domain.value_objects import PaymentMethod, ProviderCode
from modules.payments.infrastructure.db.mappers import attempt_values, payment_values, to_payment
from modules.payments.infrastructure.db.models import (
    AttemptRow,
    CascadeRouteRow,
    OutboxRow,
    PaymentRow,
    ProviderEventRow,
)

A = AttemptStatus
ACTIVE_RECONCILE = [s.value for s in (A.CREATED, A.CONFIRMING, A.PARTIALLY_PAID)]
CLOSED_RECONCILE = [s.value for s in (A.CREATE_UNKNOWN, A.SUPERSEDED, A.EXPIRED)]
# окно сверки: TTL счёта + сутки на опоздавшие платежи
RECONCILE_WINDOW = timedelta(hours=25)


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession, track: list[Payment]) -> None:
        self._s = session
        self._track = track

    async def _load(self, payment_id: UUID, for_update: bool) -> Payment | None:
        q = select(PaymentRow).where(PaymentRow.id == payment_id)
        if for_update:
            q = q.with_for_update()
        row = (
            await self._s.execute(q.execution_options(populate_existing=True))
        ).scalar_one_or_none()
        if row is None:
            return None
        attempts = (
            await self._s.execute(
                select(AttemptRow)
                .where(AttemptRow.payment_id == payment_id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
        return to_payment(row, list(attempts))

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._load(payment_id, for_update=False)

    async def get_for_update(self, payment_id: UUID) -> Payment | None:
        return await self._load(payment_id, for_update=True)

    async def find_by_idempotency(self, user_id: UUID, key: str) -> Payment | None:
        pid = (
            await self._s.execute(
                select(PaymentRow.id).where(
                    PaymentRow.user_id == user_id, PaymentRow.idempotency_key == key
                )
            )
        ).scalar_one_or_none()
        return await self.get(pid) if pid else None

    async def add(self, payment: Payment) -> None:
        try:
            async with self._s.begin_nested():
                await self._s.execute(insert(PaymentRow).values(**payment_values(payment)))
        except IntegrityError as e:
            raise DuplicatePayment() from e
        await self._upsert_attempts(payment)
        self._track.append(payment)

    async def save(self, payment: Payment) -> None:
        values = payment_values(payment) | {"version": payment.version + 1}
        res = await self._s.execute(
            update(PaymentRow)
            .where(PaymentRow.id == payment.id, PaymentRow.version == payment.version)
            .values(**values)
        )
        if res.rowcount != 1:  # type: ignore[attr-defined]
            raise ConcurrencyConflict(f"payment {payment.id} version {payment.version}")
        payment.version += 1
        await self._upsert_attempts(payment)
        self._track.append(payment)

    async def _upsert_attempts(self, payment: Payment) -> None:
        for a in payment.attempts:
            values = attempt_values(payment.id, a)
            stmt = insert(AttemptRow).values(**values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[AttemptRow.id],
                set_={k: stmt.excluded[k] for k in values if k not in ("id", "payment_id", "seq")},
            )
            try:
                async with self._s.begin_nested():
                    await self._s.execute(stmt)
            except IntegrityError as e:
                raise DuplicateProviderPaymentId(str(a.provider_payment_id)) from e

    async def find_attempt(
        self,
        provider: ProviderCode,
        provider_payment_id: str | None = None,
        attempt_id: UUID | None = None,
    ) -> tuple[UUID, UUID] | None:
        q = select(AttemptRow.payment_id, AttemptRow.id).where(AttemptRow.provider == provider)
        if provider_payment_id is not None:
            q = q.where(AttemptRow.provider_payment_id == provider_payment_id)
        elif attempt_id is not None:
            q = q.where(AttemptRow.id == attempt_id)
        else:
            return None
        row = (await self._s.execute(q)).first()
        return (row[0], row[1]) if row else None

    async def attempts_to_reconcile(
        self, now: datetime, active_after_s: int, closed_after_s: int, limit: int
    ) -> list[tuple[UUID, UUID]]:
        """Активные попытки (ждём денег) — часто и первыми; закрытые — редко, только ради
        опоздавших платежей. Иначе тысячи мёртвых попыток задерживают живые."""
        last = func.coalesce(AttemptRow.checked_at, AttemptRow.updated_at)
        is_active = AttemptRow.status.in_(ACTIVE_RECONCILE)
        q = (
            select(AttemptRow.payment_id, AttemptRow.id)
            .where(
                AttemptRow.provider_payment_id.is_not(None),
                AttemptRow.created_at > now - RECONCILE_WINDOW,
                or_(
                    and_(is_active, last <= now - timedelta(seconds=active_after_s)),
                    and_(
                        AttemptRow.status.in_(CLOSED_RECONCILE),
                        last <= now - timedelta(seconds=closed_after_s),
                    ),
                ),
            )
            .order_by(case((is_active, 0), else_=1), last)
            .limit(limit)
        )
        return [(r[0], r[1]) for r in await self._s.execute(q)]

    async def stuck_payments(self, now: datetime, older_than_s: int, limit: int) -> list[UUID]:
        q = (
            select(PaymentRow.id)
            .where(
                PaymentRow.status.in_([PaymentStatus.NEW.value, PaymentStatus.ROUTING.value]),
                PaymentRow.updated_at <= now - timedelta(seconds=older_than_s),
            )
            .order_by(PaymentRow.updated_at)
            .limit(limit)
        )
        return [r[0] for r in await self._s.execute(q)]

    async def attempts_past_expiry(
        self, now: datetime, grace_min: int, limit: int
    ) -> list[tuple[UUID, UUID]]:
        q = (
            select(AttemptRow.payment_id, AttemptRow.id)
            .where(
                AttemptRow.status == A.CREATED.value,
                AttemptRow.expires_at < now - timedelta(minutes=grace_min),
            )
            .order_by(AttemptRow.expires_at)
            .limit(limit)
        )
        return [(r[0], r[1]) for r in await self._s.execute(q)]


class SqlRouteRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def get(self, method: PaymentMethod, pay_currency: str) -> list[ProviderCode]:
        rows = (
            await self._s.execute(
                select(CascadeRouteRow.pay_currency, CascadeRouteRow.providers).where(
                    CascadeRouteRow.method == method.value,
                    CascadeRouteRow.enabled.is_(True),
                    or_(
                        CascadeRouteRow.pay_currency == pay_currency.lower(),
                        CascadeRouteRow.pay_currency == "*",
                    ),
                )
            )
        ).all()
        by_cur = {r[0]: r[1] for r in rows}
        codes = by_cur.get(pay_currency.lower()) or by_cur.get("*") or []
        return [ProviderCode(c) for c in codes]

    async def currencies(self) -> list[str]:
        rows = await self._s.execute(
            select(CascadeRouteRow.pay_currency)
            .where(CascadeRouteRow.pay_currency != "*", CascadeRouteRow.enabled.is_(True))
            .order_by(CascadeRouteRow.pay_currency)
        )
        return [r[0] for r in rows]


class SqlProviderEventInbox:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def add(
        self, provider: ProviderCode, event_hash: str, ref: ProviderEventRef, raw: str
    ) -> UUID | None:
        eid = uuid4()
        stmt = (
            insert(ProviderEventRow)
            .values(
                id=eid,
                provider=provider,
                event_hash=event_hash,
                provider_payment_id=ref.provider_payment_id,
                order_id=ref.order_id,
                raw=raw,
                received_at=func.now(),
            )
            .on_conflict_do_nothing(index_elements=["provider", "event_hash"])
            .returning(ProviderEventRow.id)
        )
        return (await self._s.execute(stmt)).scalar_one_or_none()

    async def get(self, event_id: UUID) -> InboxEvent | None:
        r = (
            await self._s.execute(select(ProviderEventRow).where(ProviderEventRow.id == event_id))
        ).scalar_one_or_none()
        if r is None:
            return None
        return InboxEvent(
            id=r.id,
            provider=ProviderCode(r.provider),
            provider_payment_id=r.provider_payment_id,
            order_id=r.order_id,
            processed_at=r.processed_at,
        )

    async def mark_processed(self, event_id: UUID, note: str | None = None) -> None:
        await self._s.execute(
            update(ProviderEventRow)
            .where(ProviderEventRow.id == event_id)
            .values(processed_at=func.now(), note=note)
        )

    async def unprocessed(self, now: datetime, older_than_s: int, limit: int) -> list[UUID]:
        q = (
            select(ProviderEventRow.id)
            .where(
                ProviderEventRow.processed_at.is_(None),
                ProviderEventRow.received_at <= now - timedelta(seconds=older_than_s),
            )
            .order_by(ProviderEventRow.received_at)
            .limit(limit)
        )
        return [r[0] for r in await self._s.execute(q)]


class SqlOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def add(self, event: DomainEvent) -> None:
        await self._s.execute(
            insert(OutboxRow).values(
                id=event.event_id,
                aggregate_id=event.payment_id,
                event_type=event.event_type,
                payload=event.model_dump(mode="json") | {"event_type": event.event_type},
                created_at=event.occurred_at,
            )
        )

    async def fetch_unpublished(self, batch: int) -> list[OutboxRecord]:
        rows = (
            await self._s.execute(
                select(OutboxRow)
                .where(OutboxRow.published_at.is_(None))
                .order_by(OutboxRow.created_at)
                .limit(batch)
                .with_for_update(skip_locked=True)
            )
        ).scalars()
        return [OutboxRecord(id=r.id, event_type=r.event_type, payload=r.payload) for r in rows]

    async def mark_published(self, ids: Sequence[UUID]) -> None:
        await self._s.execute(
            update(OutboxRow).where(OutboxRow.id.in_(list(ids))).values(published_at=func.now())
        )
