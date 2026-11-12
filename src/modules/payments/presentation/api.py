from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.auth import current_user_id
from modules.payments.application.use_cases import (
    CreatePaymentCommand,
    CreatePaymentResult,
    PaymentView,
)
from modules.payments.presentation.deps import PaymentsFacade, get_facade
from modules.payments.presentation.schemas import CreatePaymentIn

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])

User = Annotated[UUID, Depends(current_user_id)]
Facade = Annotated[PaymentsFacade, Depends(get_facade)]


def _status(result: CreatePaymentResult) -> int:
    if not result.created:
        return status.HTTP_200_OK
    return status.HTTP_202_ACCEPTED if result.deferred else status.HTTP_201_CREATED


@router.post("", response_model=PaymentView, status_code=status.HTTP_201_CREATED)
async def create_payment(
    body: CreatePaymentIn,
    response: Response,
    user_id: User,
    facade: Facade,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> PaymentView:
    if not idempotency_key or len(idempotency_key) > 128:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Idempotency-Key header (1..128) required")
    result = await facade.create(
        CreatePaymentCommand(
            user_id=user_id,
            price=body.price,
            pay_currency=body.pay_currency,
            purpose=body.purpose,
            idempotency_key=idempotency_key,
        )
    )
    response.status_code = _status(result)
    return result.view


@router.get("/{payment_id}", response_model=PaymentView)
async def get_payment(payment_id: UUID, user_id: User, facade: Facade) -> PaymentView:
    return await facade.get(user_id, payment_id)


@router.post("/{payment_id}/reinvoice", response_model=PaymentView)
async def reinvoice(
    payment_id: UUID, response: Response, user_id: User, facade: Facade
) -> PaymentView:
    result = await facade.reinvoice(user_id, payment_id)
    response.status_code = _status(result)
    return result.view
