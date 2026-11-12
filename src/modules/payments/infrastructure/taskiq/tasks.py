from typing import Any
from uuid import UUID

from taskiq import Context, TaskiqDepends

from modules.payments.infrastructure.taskiq.broker import broker


def get_container(context: Context = TaskiqDepends()) -> Any:  # noqa: B008
    return context.state.container


Container = Any
DEP = TaskiqDepends(get_container)


@broker.task(task_name="payments.process_provider_event", retry_on_error=True)
async def process_provider_event(event_id: str, c: Container = DEP) -> None:
    await c.process_event(UUID(event_id))
    await c.relay_outbox()


@broker.task(task_name="payments.continue_cascade", retry_on_error=True)
async def continue_cascade(payment_id: str, c: Container = DEP) -> None:
    pid = UUID(payment_id)
    while await c.cascade.run(pid) == "deferred":
        pass  # в воркере дедлайн HTTP-запроса не нужен — доводим каскад до конца
    await c.relay_outbox()


@broker.task(task_name="payments.reconcile", schedule=[{"cron": "* * * * *"}])
async def reconcile(c: Container = DEP) -> int:
    if not await c.try_lock("reconcile", 55):  # предыдущий запуск ещё идёт
        return 0
    n: int = await c.reconcile()
    await c.relay_outbox()
    return n


@broker.task(task_name="payments.recover", schedule=[{"cron": "* * * * *"}])
async def recover(c: Container = DEP) -> int:
    """Зависшие шаги каскада и необработанные вебхуки (review C1/I3)."""
    if not await c.try_lock("recover", 55):
        return 0
    n: int = await c.recover()
    await c.relay_outbox()
    return n


@broker.task(task_name="payments.expire_stale", schedule=[{"cron": "* * * * *"}])
async def expire_stale(c: Container = DEP) -> int:
    n: int = await c.expire_stale()
    await c.relay_outbox()
    return n


@broker.task(task_name="payments.relay_outbox", schedule=[{"cron": "* * * * *"}])
async def relay_outbox(c: Container = DEP) -> int:
    n: int = await c.relay_outbox()
    return n


@broker.task(task_name="payments.refresh_capabilities", schedule=[{"cron": "*/10 * * * *"}])
async def refresh_capabilities(c: Container = DEP) -> int:
    n: int = await c.refresh_capabilities()
    return n
