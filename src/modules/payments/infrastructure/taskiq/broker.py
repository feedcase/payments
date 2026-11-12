"""Taskiq-брокер модуля payments (реализация 1: Redis Streams с ack).

Точка входа воркера и планировщика — modules.payments.worker (там же сборка контейнера).
"""

import logging
import os
from typing import Any

from taskiq import (
    AsyncBroker,
    InMemoryBroker,
    ScheduleSource,
    SmartRetryMiddleware,
    TaskiqMessage,
    TaskiqMiddleware,
    TaskiqResult,
    TaskiqScheduler,
)
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import ListRedisScheduleSource, RedisStreamBroker

from modules.payments.infrastructure.metrics import TASK_FAILED

log = logging.getLogger(__name__)


class ErrorMetricsMiddleware(TaskiqMiddleware):
    """Лог + метрика на каждую упавшую попытку задачи (вместо таблицы failed_tasks)."""

    async def on_error(
        self, message: TaskiqMessage, result: TaskiqResult[Any], exception: BaseException
    ) -> None:
        TASK_FAILED.labels(message.task_name).inc()
        log.error("task %s failed: %r", message.task_name, exception)


MEMORY = os.environ.get("TASKIQ_BROKER") == "memory"
REDIS_URL = os.environ.get("REDIS__URL", "redis://localhost:6390/0")
# таймауты сокета: «чёрная дыра» вместо Redis не должна вешать .kiq() в HTTP-запросе
REDIS_KW: dict[str, Any] = {"socket_timeout": 2.0, "socket_connect_timeout": 2.0}
# воркер читает поток блокирующим XREAD — таймаут сокета обязан быть больше блокировки
XREAD_BLOCK_MS = 2000
BROKER_KW: dict[str, Any] = {"socket_timeout": 5.0, "socket_connect_timeout": 2.0}


def _make_broker() -> AsyncBroker:
    if MEMORY:
        return InMemoryBroker()
    return RedisStreamBroker(
        url=REDIS_URL, queue_name="payments", xread_block=XREAD_BLOCK_MS, **BROKER_KW
    )


# отложенные ретраи кладутся в Redis-источник расписания, его читает scheduler
retry_source: ScheduleSource | None = (
    None if MEMORY else ListRedisScheduleSource(REDIS_URL, prefix="payments:retry", **REDIS_KW)
)
broker: AsyncBroker = _make_broker().with_middlewares(
    SmartRetryMiddleware(
        default_retry_count=3,
        default_delay=2,
        use_jitter=True,
        use_delay_exponent=True,
        max_delay_exponent=60,
        schedule_source=retry_source,
    ),
    ErrorMetricsMiddleware(),
)
scheduler = TaskiqScheduler(
    broker, [LabelScheduleSource(broker), *([retry_source] if retry_source else [])]
)
