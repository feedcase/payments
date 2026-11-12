from enum import StrEnum


class PaymentStatus(StrEnum):
    NEW = "NEW"
    ROUTING = "ROUTING"
    AWAITING_CUSTOMER = "AWAITING_CUSTOMER"
    CONFIRMING = "CONFIRMING"
    PARTIALLY_PAID = "PARTIALLY_PAID"
    SUCCEEDED = "SUCCEEDED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


class AttemptStatus(StrEnum):
    CREATING = "CREATING"
    CREATED = "CREATED"
    CREATE_FAILED = "CREATE_FAILED"
    CREATE_UNKNOWN = "CREATE_UNKNOWN"
    CONFIRMING = "CONFIRMING"
    PARTIALLY_PAID = "PARTIALLY_PAID"
    FINISHED = "FINISHED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"
    SUPERSEDED = "SUPERSEDED"


A = AttemptStatus

ACTIVE_ATTEMPT: frozenset[AttemptStatus] = frozenset(
    {A.CREATING, A.CREATED, A.CONFIRMING, A.PARTIALLY_PAID}
)

_LATE = frozenset({A.CONFIRMING, A.PARTIALLY_PAID, A.FINISHED})

ATTEMPT_TRANSITIONS: dict[AttemptStatus, frozenset[AttemptStatus]] = {
    A.CREATING: frozenset({A.CREATED, A.CREATE_FAILED, A.CREATE_UNKNOWN, A.SUPERSEDED}),
    A.CREATE_UNKNOWN: frozenset({A.SUPERSEDED}) | _LATE,
    A.CREATED: frozenset(
        {A.CONFIRMING, A.PARTIALLY_PAID, A.FINISHED, A.EXPIRED, A.FAILED, A.SUPERSEDED}
    ),
    A.CONFIRMING: frozenset({A.PARTIALLY_PAID, A.FINISHED, A.FAILED}),
    A.PARTIALLY_PAID: frozenset({A.CONFIRMING, A.FINISHED}),
    A.SUPERSEDED: _LATE,
    A.EXPIRED: _LATE,
    A.FINISHED: frozenset(),
    A.FAILED: frozenset(),
    A.CREATE_FAILED: frozenset(),
}


def can_transition(current: AttemptStatus, target: AttemptStatus) -> bool:
    return target in ATTEMPT_TRANSITIONS[current]
