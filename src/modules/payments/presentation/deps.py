from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from fastapi import Request

from modules.payments.application.ports import ProviderRegistry, UnitOfWorkFactory
from modules.payments.application.use_cases import CreatePayment, GetPayment, Reinvoice


@dataclass(frozen=True)
class PaymentsFacade:
    """Всё, что нужно HTTP-слою модуля; собирается в container.py."""

    create: CreatePayment
    get: GetPayment
    reinvoice: Reinvoice
    registry: ProviderRegistry
    uow_factory: UnitOfWorkFactory
    enqueue_provider_event: Callable[[UUID], Awaitable[None]]


def get_facade(request: Request) -> PaymentsFacade:
    facade: PaymentsFacade = request.app.state.container.facade
    return facade
