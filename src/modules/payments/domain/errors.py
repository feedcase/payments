from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class FailureClass(StrEnum):
    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    TIMEOUT_UNKNOWN = "TIMEOUT_UNKNOWN"
    UNSUPPORTED = "UNSUPPORTED"
    INVALID_REQUEST = "INVALID_REQUEST"
    LOCAL_LIMIT = "LOCAL_LIMIT"
    CUSTOMER_FAILED = "CUSTOMER_FAILED"


class AttemptFailure(BaseModel):
    model_config = ConfigDict(frozen=True)

    cls: FailureClass
    code: str
    message: str = ""


class DomainError(Exception):
    """Нарушение инварианта агрегата."""
