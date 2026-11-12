from app.settings import Settings


def test_settings_defaults(monkeypatch):
    monkeypatch.setenv("DB__URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("REDIS__URL", "redis://localhost:6379/0")
    s = Settings()
    assert s.cascade.deadline_s == 15.0
    assert s.cascade.min_step_s == 3.0
    assert s.breaker.min_calls == 20
    assert s.breaker.failure_ratio == 0.5
    assert s.nowpayments.base_url.endswith("/v1")
    assert s.coingate.base_url.endswith("/v2")
    assert s.rate_limit_per_min == 10


def test_mocks_from_json_env(monkeypatch):
    monkeypatch.setenv("DB__URL", "x")
    monkeypatch.setenv("REDIS__URL", "y")
    monkeypatch.setenv("MOCKS", '[{"code":"mock_a","base_url":"http://m/mock_a","secret":"s"}]')
    s = Settings()
    assert s.mocks[0].code == "mock_a"


def test_inline_queue_rejected_outside_dev(monkeypatch):
    """Review I6: inline-очередь без расписания недопустима вне dev/test."""
    import pytest
    from pydantic import ValidationError

    monkeypatch.setenv("DB__URL", "x")
    monkeypatch.setenv("REDIS__URL", "y")
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("TASK_QUEUE", "inline")
    with pytest.raises(ValidationError):
        Settings()
    monkeypatch.setenv("TASK_QUEUE", "taskiq")
    assert Settings().task_queue == "taskiq"
