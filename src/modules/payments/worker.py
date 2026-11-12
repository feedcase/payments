"""Composition root фоновых процессов модуля payments.

taskiq worker modules.payments.worker:broker
taskiq scheduler modules.payments.worker:scheduler
"""

from taskiq import TaskiqEvents, TaskiqState

from modules.payments.container import Container
from modules.payments.infrastructure.taskiq import tasks
from modules.payments.infrastructure.taskiq.broker import broker, scheduler
from modules.payments.infrastructure.taskiq.dispatcher import TaskiqTaskQueue

__all__ = ["broker", "scheduler", "tasks"]


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def _startup(state: TaskiqState) -> None:
    if getattr(state, "container", None) is not None:  # уже собран (тесты/повторный старт)
        return
    from app.settings import get_settings

    container = Container(get_settings(), task_queue=TaskiqTaskQueue())
    await container.startup()
    state.container = container
    state.owns_container = True


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def _shutdown(state: TaskiqState) -> None:
    if getattr(state, "owns_container", False):
        await state.container.shutdown()
