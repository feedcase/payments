from enum import StrEnum

from modules.payments.domain.errors import FailureClass


class Decision(StrEnum):
    DONE = "DONE"
    NEXT_PROVIDER = "NEXT_PROVIDER"
    STOP = "STOP"


PENALIZED: frozenset[FailureClass] = frozenset(
    {FailureClass.PROVIDER_UNAVAILABLE, FailureClass.PROVIDER_ERROR, FailureClass.TIMEOUT_UNKNOWN}
)


def decide_on_create_failure(cls: FailureClass) -> Decision:
    """Таблица 2.4 спеки для этапа создания криптосчёта.

    Для push-платежей каскад безопасен при любой инфраструктурной ошибке, включая таймаут:
    покупатель не видел адреса из неудачной попытки. Останавливаемся только на ошибке в
    наших данных / конфиге.
    """
    if cls is FailureClass.INVALID_REQUEST:
        return Decision.STOP
    return Decision.NEXT_PROVIDER
