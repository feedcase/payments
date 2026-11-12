"""payments: initial schema

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision = "0001"
down_revision = None

AMOUNT = sa.Numeric(36, 18)
TS = sa.DateTime(timezone=True)
NON_FINAL = "'CREATED','CONFIRMING','PARTIALLY_PAID','CREATE_UNKNOWN','SUPERSEDED','EXPIRED'"


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid, primary_key=True),
        sa.Column("user_id", sa.Uuid, nullable=False),
        sa.Column("price_amount", AMOUNT, nullable=False),
        sa.Column("price_currency", sa.String(16), nullable=False),
        sa.Column("pay_currency", sa.String(32), nullable=False),
        sa.Column("method", sa.String(16), nullable=False),
        sa.Column("purpose_domain", sa.String(64), nullable=False),
        sa.Column("purpose_ref", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("received_total", AMOUNT, nullable=False),
        sa.Column("round", sa.Integer, nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("failure_reason", sa.Text),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.UniqueConstraint(
            "user_id", "idempotency_key", name="uq_payments_user_id_idempotency_key"
        ),
    )
    op.create_index("ix_payments_status_updated_at", "payments", ["status", "updated_at"])

    op.create_table(
        "payment_attempts",
        sa.Column("id", sa.Uuid, primary_key=True),
        sa.Column(
            "payment_id", sa.Uuid, sa.ForeignKey("payments.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("round", sa.Integer, nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("provider_payment_id", sa.String(128)),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("failure_class", sa.String(32)),
        sa.Column("failure_code", sa.String(64)),
        sa.Column("failure_message", sa.Text),
        sa.Column("checkout", pg.JSONB),
        sa.Column("expires_at", TS),
        sa.Column("received_amount", AMOUNT),
        sa.Column("received_currency", sa.String(32)),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.Column("checked_at", TS),
        sa.UniqueConstraint("payment_id", "seq", name="uq_payment_attempts_payment_id_seq"),
        sa.UniqueConstraint(
            "provider",
            "provider_payment_id",
            name="uq_payment_attempts_provider_provider_payment_id",
        ),
    )
    op.create_index(
        "ix_payment_attempts_reconcile",
        "payment_attempts",
        ["status", "updated_at"],
        postgresql_where=sa.text(f"status IN ({NON_FINAL})"),
    )
    op.create_index(
        "ix_payment_attempts_expires_at",
        "payment_attempts",
        ["expires_at"],
        postgresql_where=sa.text("status = 'CREATED'"),
    )

    op.create_table(
        "provider_events",
        sa.Column("id", sa.Uuid, primary_key=True),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("event_hash", sa.String(64), nullable=False),
        sa.Column("provider_payment_id", sa.String(128)),
        sa.Column("order_id", sa.Uuid),
        sa.Column("raw", sa.Text, nullable=False),
        sa.Column("received_at", TS, nullable=False),
        sa.Column("processed_at", TS),
        sa.Column("note", sa.Text),
        sa.UniqueConstraint(
            "provider", "event_hash", name="uq_provider_events_provider_event_hash"
        ),
    )

    op.create_table(
        "payments_outbox",
        sa.Column("id", sa.Uuid, primary_key=True),
        sa.Column("aggregate_id", sa.Uuid, nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", pg.JSONB, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("published_at", TS),
    )
    op.create_index(
        "ix_payments_outbox_unpublished",
        "payments_outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )

    routes = op.create_table(
        "cascade_routes",
        sa.Column("method", sa.String(16), primary_key=True),
        sa.Column("pay_currency", sa.String(32), primary_key=True),
        sa.Column("providers", pg.ARRAY(sa.Text), nullable=False),
        sa.Column("strategy", sa.String(32), nullable=False, server_default="priority"),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default=sa.text("true")),
    )
    op.bulk_insert(
        routes,
        [
            {
                "method": "CRYPTO",
                "pay_currency": "*",
                "providers": ["nowpayments", "coingate"],
                "strategy": "priority",
                "enabled": True,
            }
        ],
    )


def downgrade() -> None:
    for t in (
        "cascade_routes",
        "payments_outbox",
        "provider_events",
        "payment_attempts",
        "payments",
    ):
        op.drop_table(t)
