from types import SimpleNamespace
from uuid import uuid4

import pytest

from modules.payments.infrastructure.taskiq import tasks
from modules.payments.infrastructure.taskiq.broker import broker
from modules.payments.infrastructure.taskiq.dispatcher import TaskiqTaskQueue


class Recorder:
    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []
        self.deferred_left = 0

    def fn(self, name, result=None):
        async def call(*args):
            self.calls.append((name, args))
            if name == "cascade.run" and self.deferred_left:
                self.deferred_left -= 1
                return "deferred"
            return result

        return call


@pytest.fixture
async def rec():
    r = Recorder()
    broker.state.container = SimpleNamespace(
        cascade=SimpleNamespace(run=r.fn("cascade.run", "completed")),
        process_event=r.fn("process_event"),
        reconcile=r.fn("reconcile", 0),
        expire_stale=r.fn("expire_stale", 0),
        relay_outbox=r.fn("relay_outbox", 0),
        refresh_capabilities=r.fn("refresh_capabilities", 0),
        recover=r.fn("recover", 0),
        try_lock=r.fn("try_lock", True),
    )
    await broker.startup()
    yield r
    await broker.shutdown()


async def test_continue_cascade_calls_orchestrator(rec):
    pid = uuid4()
    rec.deferred_left = 1
    await tasks.continue_cascade.kiq(str(pid))
    await broker.wait_all()
    runs = [c for c in rec.calls if c[0] == "cascade.run"]
    assert runs == [("cascade.run", (pid,)), ("cascade.run", (pid,))]
    assert ("relay_outbox", ()) in rec.calls


async def test_process_event_task_calls_use_case(rec):
    eid = uuid4()
    await tasks.process_provider_event.kiq(str(eid))
    await broker.wait_all()
    assert ("process_event", (eid,)) in rec.calls
    assert ("relay_outbox", ()) in rec.calls


@pytest.mark.parametrize(
    ("task", "cron"),
    [
        (tasks.reconcile, "* * * * *"),
        (tasks.expire_stale, "* * * * *"),
        (tasks.relay_outbox, "* * * * *"),
        (tasks.refresh_capabilities, "*/10 * * * *"),
        (tasks.recover, "* * * * *"),
    ],
)
def test_schedules_registered(task, cron):
    assert task.labels["schedule"] == [{"cron": cron}]


def test_task_names():
    assert tasks.process_provider_event.task_name == "payments.process_provider_event"
    assert tasks.continue_cascade.task_name == "payments.continue_cascade"


async def test_dispatcher_kiqs(rec):
    q = TaskiqTaskQueue()
    pid, eid = uuid4(), uuid4()
    await q.continue_cascade(pid)
    await q.process_provider_event(eid)
    await broker.wait_all()
    names = [c[0] for c in rec.calls]
    assert "cascade.run" in names and "process_event" in names


async def test_periodic_tasks_call_use_cases(rec):
    for t in (tasks.reconcile, tasks.expire_stale, tasks.refresh_capabilities):
        await t.kiq()
    await broker.wait_all()
    names = {c[0] for c in rec.calls}
    assert {"reconcile", "expire_stale", "refresh_capabilities"} <= names


def test_worker_entrypoint_registers_tasks():
    from modules.payments import worker

    assert "payments.reconcile" in worker.broker.get_all_tasks()


async def test_reconcile_skips_when_locked(rec):
    """Review I4: перекрывающиеся запуски сверки не дублируют опрос провайдеров."""

    async def locked(name, ttl):
        rec.calls.append(("try_lock", (name, ttl)))
        return False

    broker.state.container.try_lock = locked
    await tasks.reconcile.kiq()
    await broker.wait_all()
    assert "reconcile" not in [c[0] for c in rec.calls]


async def test_recover_task_calls_use_case(rec):
    await tasks.recover.kiq()
    await broker.wait_all()
    assert "recover" in [c[0] for c in rec.calls]


def test_retry_middleware_has_backoff():
    """Review I5: ретраи с экспоненциальной паузой, а не мгновенно."""
    from taskiq import SmartRetryMiddleware

    mw = [m for m in broker.middlewares if isinstance(m, SmartRetryMiddleware)]
    assert mw and mw[0].use_delay_exponent and mw[0].default_delay >= 1


def test_broker_socket_timeout_exceeds_xread_block():
    """Регрессия: socket_timeout <= xread_block роняет слушатель воркера (TimeoutError)."""
    from modules.payments.infrastructure.taskiq.broker import BROKER_KW, XREAD_BLOCK_MS

    assert BROKER_KW["socket_timeout"] > XREAD_BLOCK_MS / 1000
