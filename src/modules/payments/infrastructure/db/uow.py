from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from modules.payments.application.ports import (
    OutboxRepository,
    PaymentRepository,
    ProviderEventInbox,
    RouteRepository,
)
from modules.payments.domain.entities import Payment
from modules.payments.infrastructure.db.repositories import (
    SqlOutboxRepository,
    SqlPaymentRepository,
    SqlProviderEventInbox,
    SqlRouteRepository,
)


class SqlAlchemyUnitOfWork:
    """Одна транзакция = один UoW. События сохранённых агрегатов пишутся в outbox
    в той же транзакции при commit (transactional outbox)."""

    session: AsyncSession
    # типы портов, а не реализаций: атрибуты Protocol инвариантны
    payments: PaymentRepository
    routes: RouteRepository
    events: ProviderEventInbox
    outbox: OutboxRepository

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sm = sessionmaker

    async def __aenter__(self) -> Self:
        self.session = self._sm()
        self._tracked: list[Payment] = []
        self.payments = SqlPaymentRepository(self.session, self._tracked)
        self.routes = SqlRouteRepository(self.session)
        self.events = SqlProviderEventInbox(self.session)
        self.outbox = SqlOutboxRepository(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            await self.session.rollback()
        finally:
            await self.session.close()

    async def commit(self) -> None:
        for p in self._tracked:
            for event in p.pull_events():
                await self.outbox.add(event)
        self._tracked.clear()
        await self.session.commit()
