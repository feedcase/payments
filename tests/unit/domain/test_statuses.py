import pytest

from modules.payments.domain.statuses import AttemptStatus, can_transition


@pytest.mark.parametrize("target", list(AttemptStatus))
def test_finished_is_terminal(target):
    assert not can_transition(AttemptStatus.FINISHED, target)


def test_superseded_accepts_late_finish():
    assert can_transition(AttemptStatus.SUPERSEDED, AttemptStatus.FINISHED)


def test_expired_accepts_late_finish():
    assert can_transition(AttemptStatus.EXPIRED, AttemptStatus.FINISHED)


def test_confirming_cannot_go_back_to_created():
    assert not can_transition(AttemptStatus.CONFIRMING, AttemptStatus.CREATED)
