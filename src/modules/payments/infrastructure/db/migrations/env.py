import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from modules.payments.infrastructure.db.models import Base

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

URL = os.environ["DB__URL"]
VERSION_TABLE = config.get_main_option("version_table") or "alembic_version_payments"


def run_offline() -> None:
    context.configure(url=URL, target_metadata=Base.metadata, version_table=VERSION_TABLE)
    with context.begin_transaction():
        context.run_migrations()


def _sync(conn) -> None:  # type: ignore[no-untyped-def]
    context.configure(connection=conn, target_metadata=Base.metadata, version_table=VERSION_TABLE)
    with context.begin_transaction():
        context.run_migrations()


async def run_online() -> None:
    engine = create_async_engine(URL)
    async with engine.connect() as conn:
        await conn.run_sync(_sync)
    await engine.dispose()


if context.is_offline_mode():
    run_offline()
else:
    asyncio.run(run_online())
