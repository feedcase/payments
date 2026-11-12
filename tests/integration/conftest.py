import asyncio
import os
import subprocess
import sys

import asyncpg
import pytest
from sqlalchemy import text

from app.db import make_engine, make_sessionmaker
from modules.payments.infrastructure.db.uow import SqlAlchemyUnitOfWork

ADMIN_DSN = os.environ.get(
    "TEST_PG_ADMIN", "postgresql://payments:payments@localhost:5433/payments"
)
TEST_DB_URL = os.environ.get(
    "TEST_DB__URL", "postgresql+asyncpg://payments:payments@localhost:5433/payments_test"
)
TEST_REDIS_URL = os.environ.get("TEST_REDIS__URL", "redis://localhost:6390/15")


async def _ensure_db() -> None:
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname='payments_test'")
        if not exists:
            await conn.execute("CREATE DATABASE payments_test")
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def migrated_db() -> str:
    asyncio.run(_ensure_db())
    env = {**os.environ, "DB__URL": TEST_DB_URL}
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"], check=True, env=env, cwd=os.getcwd()
    )
    return TEST_DB_URL


@pytest.fixture
async def sessionmaker(migrated_db):
    engine = make_engine(migrated_db)
    async with engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE payment_attempts, payments, provider_events, payments_outbox CASCADE")
        )
        await conn.execute(text("DELETE FROM cascade_routes WHERE pay_currency <> '*'"))
    yield make_sessionmaker(engine)
    await engine.dispose()


@pytest.fixture
def uow_factory(sessionmaker):
    return lambda: SqlAlchemyUnitOfWork(sessionmaker)
