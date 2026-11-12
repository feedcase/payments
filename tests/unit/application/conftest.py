import pytest

from tests.fakes import Env


@pytest.fixture
def make_env():
    return Env
