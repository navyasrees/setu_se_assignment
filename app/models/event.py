"""Append-only raw event log.

event_id is the primary key — that alone guarantees idempotency: re-ingesting the
same event_id hits a unique-constraint violation, which we translate into a clean
'already accepted' response.
"""

import enum
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import CHAR, DateTime, Enum, ForeignKey, Index, Numeric, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class EventType(str, enum.Enum):
    PAYMENT_INITIATED = "payment_initiated"
    PAYMENT_PROCESSED = "payment_processed"
    PAYMENT_FAILED = "payment_failed"
    SETTLED = "settled"


class Event(Base):
    __tablename__ = "events"

    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    transaction_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    merchant_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("merchants.merchant_id"), nullable=False
    )
    event_type: Mapped[EventType] = mapped_column(
        Enum(EventType, name="event_type_enum"), nullable=False
    )
    amount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False, default="INR")

    # When the event happened in the source system.
    event_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # When we actually ingested it. Difference tells you about lag / late arrivals.
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    # Keep the original payload for audit / debugging / future fields.
    raw_payload: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        Index("idx_events_transaction", "transaction_id", "event_timestamp"),
        Index("idx_events_merchant_time", "merchant_id", "event_timestamp"),
        Index("idx_events_type_time", "event_type", "event_timestamp"),
    )
