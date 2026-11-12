import hashlib
import logging

from fastapi import APIRouter, HTTPException, Request, status

from modules.payments.application.ports import InvalidWebhook
from modules.payments.domain.value_objects import ProviderCode
from modules.payments.presentation.deps import get_facade

log = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/{provider}")
async def provider_webhook(provider: str, request: Request) -> dict[str, bool]:
    """Проверка подписи → inbox → 200 → асинхронная обработка (§2.6). Тело не применяется
    напрямую: задача перезапросит статус у провайдера."""
    facade = get_facade(request)
    code = ProviderCode(provider)
    if not facade.registry.has(code):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown provider")
    body = await request.body()
    try:
        ref = facade.registry.get(code).parse_webhook(request.headers, body)
    except InvalidWebhook as e:
        log.warning("rejected webhook from %s: %s", provider, e)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid webhook") from e
    async with facade.uow_factory() as uow:
        event_id = await uow.events.add(
            code, hashlib.sha256(body).hexdigest(), ref, body.decode(errors="replace")
        )
        await uow.commit()
    if event_id is not None:
        try:
            await facade.enqueue_provider_event(event_id)
        except Exception:  # событие уже в inbox — его обработает RecoverStuck
            log.exception("cannot enqueue provider event %s", event_id)
    return {"ok": True}
