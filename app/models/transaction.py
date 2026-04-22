"""Derived transaction state — one row per transaction_id.

Maintained by the event-ingestion service. Every read endpoint queries this table,
never the raw event log, because aggregating millions of events on every request
would not scale.
"""

import enum
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import CHAR, DateTime, Enum, ForeignKey, Index, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TransactionStatus(str, enum.Enum):
    INITIATED = "initiated"
    PROCESSED = "processed"
    FAILED = "failed"
    SETTLED = "settled"


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    merchant_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("merchants.merchant_id"), nullable=False
    )
    amount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False, default="INR")

    current_status: Mapped[TransactionStatus] = mapped_column(
        Enum(TransactionStatus, name="txn_status_enum"), nullable=False
    )

    # Timestamps for each lifecycle stage. Nullable because not every transaction
    # reaches every stage. Having them as separate columns turns discrepancy
    # detection into trivial WHERE clauses.
    initiated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    settled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("idx_txn_merchant_status", "merchant_id", "current_status"),
        Index("idx_txn_initiated_at", "initiated_at"),
        Index("idx_txn_status_updated", "current_status", "updated_at"),
    )
