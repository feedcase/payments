"""provider_events: индекс для повторной обработки необработанных вебхуков

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"


def upgrade() -> None:
    op.create_index(
        "ix_provider_events_unprocessed",
        "provider_events",
        ["received_at"],
        postgresql_where=sa.text("processed_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_provider_events_unprocessed", table_name="provider_events")
