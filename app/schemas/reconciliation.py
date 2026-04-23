"""Pydantic schemas for the /reconciliation endpoints."""

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

from app.models.transaction import TransactionStatus


# ---------------------------------------------------------------------------
#   /reconciliation/summary
# ---------------------------------------------------------------------------


class SummaryGroup(BaseModel):
    """One row in the grouped breakdown: (merchant × date × status)."""

    merchant_id: str
    merchant_name: str
    date: date
    status: TransactionStatus
    count: int
    total_amount: Decimal


class StatusBreakdown(BaseModel):
    """Per-status aggregate inside the filter-wide totals."""

    count: int
    total_amount: Decimal


class SummaryTotals(BaseModel):
    """Top-line snapshot over the entire filtered set.

    Kept flat + simple so it renders as a header card without the client doing
    any folding. The per-status dict is keyed by TransactionStatus — that gives
    four predictable keys in the JSON: initiated / processed / failed / settled.
    """

    transaction_count: int
    total_amount: Decimal
    by_status: Dict[TransactionStatus, StatusBreakdown]


class SummaryFilters(BaseModel):
    """Echo of the filters the caller passed — lets the client confirm scope."""

    merchant_id: Optional[str] = None
    status: Optional[TransactionStatus] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None


class ReconciliationSummaryResponse(BaseModel):
    filters: SummaryFilters
    totals: SummaryTotals
    groups: List[SummaryGroup]


# ---------------------------------------------------------------------------
#   /reconciliation/discrepancies
# ---------------------------------------------------------------------------


class DiscrepancyType(str, enum.Enum):
    """Types of discrepancies the detector flags.

    Kept as a string enum (rather than free-form) so clients can switch on it
    without fuzzy matching, and so OpenAPI docs enumerate the values.
    """

    CONFLICTING_EVENTS = "conflicting_events"
    STUCK_IN_PROCESSED = "stuck_in_processed"
    STUCK_IN_INITIATED = "stuck_in_initiated"


class DiscrepancyItem(BaseModel):
    """One flagged transaction. Shape mirrors TransactionListItem so clients
    can reuse the same rendering component, with a discriminator on `type`."""

    type: DiscrepancyType
    transaction_id: UUID
    merchant_id: str
    merchant_name: str
    amount: Decimal
    currency: str
    current_status: TransactionStatus

    initiated_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    settled_at: Optional[datetime] = None

    # Human-readable explanation of *why* this row was flagged. The client can
    # show this directly; no need for a translation table on their side.
    detail: str


class DiscrepancySummary(BaseModel):
    total: int
    by_type: Dict[DiscrepancyType, int]


class DiscrepancyFilters(BaseModel):
    merchant_id: Optional[str] = None
    type: Optional[DiscrepancyType] = None
    processed_stale_hours: int = 24
    initiated_stale_hours: int = 1


class ReconciliationDiscrepanciesResponse(BaseModel):
    filters: DiscrepancyFilters
    summary: DiscrepancySummary
    discrepancies: List[DiscrepancyItem]
