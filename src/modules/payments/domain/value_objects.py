from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import NewType

from pydantic import BaseModel, ConfigDict, field_validator

ProviderCode = NewType("ProviderCode", str)


class PaymentMethod(StrEnum):
    CRYPTO = "CRYPTO"


class Money(BaseModel):
    model_config = ConfigDict(frozen=True)

    amount: Decimal
    currency: str

    @field_validator("amount", mode="before")
    @classmethod
    def _no_float(cls, v: object) -> object:
        if isinstance(v, float):
            raise ValueError("float is not allowed for money; pass str or Decimal")
        return v

    @field_validator("currency")
    @classmethod
    def _lower(cls, v: str) -> str:
        return v.lower()


class Purpose(BaseModel):
    model_config = ConfigDict(frozen=True)

    domain: str
    ref: str


class Checkout(BaseModel):
    model_config = ConfigDict(frozen=True)

    pay_address: str | None = None
    pay_amount: Decimal | None = None
    pay_currency: str | None = None
    invoice_url: str | None = None
    expires_at: datetime
