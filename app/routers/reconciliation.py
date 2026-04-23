"""HTTP layer for reconciliation endpoints. Thin; logic lives in services."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.transaction import TransactionStatus
from app.schemas.reconciliation import (
    ReconciliationSummaryResponse,
    SummaryFilters,
)
from app.services import reconciliation

router = APIRouter(prefix="/reconciliation", tags=["reconciliation"])


@router.get(
    "/summary",
    response_model=ReconciliationSummaryResponse,
    summary="Aggregate transaction counts + totals grouped by merchant × date × status",
)
def get_summary(
    merchant_id: Optional[str] = Query(None, description="Exact-match merchant ID"),
    status_filter: Optional[TransactionStatus] = Query(
        None, alias="status", description="Exact-match current status"
    ),
    date_from: Optional[datetime] = Query(
        None, description="Lower bound on initiated_at (inclusive)"
    ),
    date_to: Optional[datetime] = Query(
        None, description="Upper bound on initiated_at (inclusive)"
    ),
    db: Session = Depends(get_db),
) -> ReconciliationSummaryResponse:
    """Return filter-wide totals plus a per-(merchant, date, status) breakdown.

    Dates are bucketed on UTC-day boundaries. No pagination: the grouped
    result is bounded by merchants × days × 4 statuses, which stays small
    even over a full year.
    """

    # Same cross-field validation as /transactions — kept at the HTTP layer so
    # the service stays ignorant of FastAPI/HTTP.
    if date_from is not None and date_to is not None and date_from > date_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="date_from must be <= date_to",
        )

    filters = SummaryFilters(
        merchant_id=merchant_id,
        status=status_filter,
        date_from=date_from,
        date_to=date_to,
    )
    return reconciliation.summary(db, filters)
