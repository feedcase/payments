from pydantic import BaseModel, ConfigDict

from modules.payments.domain.value_objects import Money, PaymentMethod, Purpose


class CreatePaymentIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price: Money  # amount строкой: float запрещён валидатором Money
    pay_currency: str
    method: PaymentMethod = PaymentMethod.CRYPTO
    purpose: Purpose


class ErrorOut(BaseModel):
    error: str
    detail: str | None = None
