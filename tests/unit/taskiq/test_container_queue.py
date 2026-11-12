from app.settings import DbSettings, RedisSettings, Settings
from modules.payments.container import Container
from modules.payments.infrastructure.task_queue import InlineTaskQueue
from modules.payments.infrastructure.taskiq.dispatcher import TaskiqTaskQueue


def settings(mode):
    return Settings(db=DbSettings(url="x"), redis=RedisSettings(url="y"), task_queue=mode)


def test_container_uses_taskiq_queue_when_configured():
    assert isinstance(Container(settings("taskiq"))._make_queue(), TaskiqTaskQueue)


def test_container_uses_inline_queue_by_default():
    assert isinstance(Container(settings("inline"))._make_queue(), InlineTaskQueue)


async def test_inline_continue_cascade_runs_until_completed():
    """Review I6: inline-продолжение не бросает платёж после второго deferred."""
    from types import SimpleNamespace

    c = Container(settings("inline"))
    outcomes = ["deferred", "deferred", "completed"]

    async def run(pid):
        return outcomes.pop(0)

    c.cascade = SimpleNamespace(run=run)
    q = c._make_queue()
    await q.continue_cascade(__import__("uuid").uuid4())
    await q.drain()
    assert outcomes == []
