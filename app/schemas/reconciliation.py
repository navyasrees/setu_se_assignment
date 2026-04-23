"""Pydantic schemas for the /reconciliation endpoints."""

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import BaseModel

from app.models.transaction import TransactionStatus


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
