import pytest

from modules.payments.domain.errors import FailureClass
from modules.payments.domain.policies import PENALIZED, Decision, decide_on_create_failure


@pytest.mark.parametrize(
    ("cls", "expected"),
    [
        (FailureClass.PROVIDER_UNAVAILABLE, Decision.NEXT_PROVIDER),
        (FailureClass.PROVIDER_ERROR, Decision.NEXT_PROVIDER),
        (FailureClass.TIMEOUT_UNKNOWN, Decision.NEXT_PROVIDER),
        (FailureClass.UNSUPPORTED, Decision.NEXT_PROVIDER),
        (FailureClass.LOCAL_LIMIT, Decision.NEXT_PROVIDER),
        (FailureClass.CUSTOMER_FAILED, Decision.NEXT_PROVIDER),
        (FailureClass.INVALID_REQUEST, Decision.STOP),
    ],
)
def test_decision_table(cls, expected):
    assert decide_on_create_failure(cls) is expected


def test_penalized_set():
    assert {
        FailureClass.PROVIDER_UNAVAILABLE,
        FailureClass.PROVIDER_ERROR,
        FailureClass.TIMEOUT_UNKNOWN,
    } == PENALIZED
