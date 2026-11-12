from decimal import Decimal

import pytest
from pydantic import ValidationError

from modules.payments.domain.value_objects import Money


def test_money_rejects_float():
    with pytest.raises(ValidationError):
        Money(amount=9.99, currency="USD")


def test_money_accepts_string_and_decimal():
    assert Money(amount="9.99", currency="usd").amount == Decimal("9.99")
    assert Money(amount=Decimal("1"), currency="usd").amount == Decimal("1")


def test_money_lowercases_currency():
    assert Money(amount="1", currency="USD").currency == "usd"
