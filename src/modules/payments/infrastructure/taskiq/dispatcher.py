from uuid import UUID


class TaskiqTaskQueue:
    """TaskQueue поверх Taskiq: фоновые шаги уходят воркерам через Redis Streams."""

    async def continue_cascade(self, payment_id: UUID) -> None:
        from modules.payments.infrastructure.taskiq.tasks import continue_cascade

        await continue_cascade.kiq(str(payment_id))

    async def process_provider_event(self, event_id: UUID) -> None:
        from modules.payments.infrastructure.taskiq.tasks import process_provider_event

        await process_provider_event.kiq(str(event_id))

    async def relay_outbox(self) -> None:
        from modules.payments.infrastructure.taskiq.tasks import relay_outbox

        await relay_outbox.kiq()
