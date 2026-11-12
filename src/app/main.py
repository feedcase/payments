import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from modules.payments.application.errors import (
    IdempotencyMismatch,
    InvalidState,
    NotFound,
    RateLimited,
)
from modules.payments.presentation import api, webhooks

ERRORS: dict[type[Exception], tuple[int, str]] = {
    IdempotencyMismatch: (422, "idempotency_key_reused"),
    RateLimited: (429, "rate_limited"),
    NotFound: (404, "not_found"),
    InvalidState: (409, "invalid_state"),
}


def create_app(container: Any | None = None) -> FastAPI:
    if container is None:
        from app.settings import get_settings
        from modules.payments.container import Container

        container = Container(get_settings())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if hasattr(container, "startup"):
            await container.startup()
        try:
            yield
        finally:
            if hasattr(container, "shutdown"):
                await container.shutdown()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    app = FastAPI(title="payments-cascade", lifespan=lifespan)
    app.state.container = container
    app.state.jwt = container.jwt

    for exc_type, (code, name) in ERRORS.items():

        async def handler(_: Request, exc: Exception, code: int = code, name: str = name):
            return JSONResponse({"error": name, "detail": str(exc) or None}, status_code=code)

        app.add_exception_handler(exc_type, handler)

    app.include_router(api.router)
    app.include_router(webhooks.router)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)

    return app
