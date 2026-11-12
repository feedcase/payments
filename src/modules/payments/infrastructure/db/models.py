from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

AMOUNT = Numeric(36, 18)
TS = DateTime(timezone=True)
NON_FINAL_ATTEMPT = (
    "'CREATED','CONFIRMING','PARTIALLY_PAID','CREATE_UNKNOWN','SUPERSEDED','EXPIRED'"
)


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_N_name)s",
            "uq": "uq_%(table_name)s_%(column_0_N_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key"),
        Index("ix_payments_status_updated_at", "status", "updated_at"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    user_id: Mapped[UUID]
    price_amount: Mapped[Decimal] = mapped_column(AMOUNT)
    price_currency: Mapped[str] = mapped_column(String(16))
    pay_currency: Mapped[str] = mapped_column(String(32))
    method: Mapped[str] = mapped_column(String(16))
    purpose_domain: Mapped[str] = mapped_column(String(64))
    purpose_ref: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32))
    received_total: Mapped[Decimal] = mapped_column(AMOUNT)
    round: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    failure_reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(TS)
    updated_at: Mapped[datetime] = mapped_column(TS)


class AttemptRow(Base):
    __tablename__ = "payment_attempts"
    __table_args__ = (
        UniqueConstraint("payment_id", "seq"),
        UniqueConstraint("provider", "provider_payment_id"),
        Index(
            "ix_payment_attempts_reconcile",
            "status",
            "updated_at",
            postgresql_where=text(f"status IN ({NON_FINAL_ATTEMPT})"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id", ondelete="CASCADE"))
    seq: Mapped[int] = mapped_column(Integer)
    round: Mapped[int] = mapped_column(Integer)
    provider: Mapped[str] = mapped_column(String(32))
    provider_payment_id: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32))
    failure_class: Mapped[str | None] = mapped_column(String(32))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    failure_message: Mapped[str | None] = mapped_column(Text)
    checkout: Mapped[dict | None] = mapped_column(JSONB)
    expires_at: Mapped[datetime | None] = mapped_column(TS)
    received_amount: Mapped[Decimal | None] = mapped_column(AMOUNT)
    received_currency: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(TS)
    updated_at: Mapped[datetime] = mapped_column(TS)
    checked_at: Mapped[datetime | None] = mapped_column(TS)


class ProviderEventRow(Base):
    __tablename__ = "provider_events"
    __table_args__ = (UniqueConstraint("provider", "event_hash"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(32))
    event_hash: Mapped[str] = mapped_column(String(64))
    provider_payment_id: Mapped[str | None] = mapped_column(String(128))
    order_id: Mapped[UUID | None]
    raw: Mapped[str] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(TS)
    processed_at: Mapped[datetime | None] = mapped_column(TS)
    note: Mapped[str | None] = mapped_column(Text)


class OutboxRow(Base):
    __tablename__ = "payments_outbox"
    __table_args__ = (
        Index(
            "ix_payments_outbox_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    aggregate_id: Mapped[UUID]
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(TS)
    published_at: Mapped[datetime | None] = mapped_column(TS)


class CascadeRouteRow(Base):
    __tablename__ = "cascade_routes"

    method: Mapped[str] = mapped_column(String(16), primary_key=True)
    pay_currency: Mapped[str] = mapped_column(String(32), primary_key=True)
    providers: Mapped[list[str]] = mapped_column(ARRAY(Text))
    strategy: Mapped[str] = mapped_column(String(32), server_default="priority")
    enabled: Mapped[bool] = mapped_column(server_default=text("true"))
