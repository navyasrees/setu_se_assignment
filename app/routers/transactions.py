"""HTTP layer for transaction read endpoints. Thin; logic lives in services."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.transaction import TransactionStatus
from app.schemas.transaction import TransactionListFilters, TransactionListResponse
from app.services import transaction_query
from app.services.transaction_query import SORT_FIELDS

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.get(
    "",
    response_model=TransactionListResponse,
    summary="List transactions with filters, sorting and pagination",
)
def list_transactions(
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
    sort: str = Query(
        "-initiated_at",
        description=(
            "Sort field with optional '-' prefix for descending. "
            "Allowed: initiated_at, amount, current_status, updated_at."
        ),
    ),
    limit: int = Query(50, ge=1, le=200, description="Page size (max 200)"),
    offset: int = Query(0, ge=0, description="Offset-based pagination cursor"),
    db: Session = Depends(get_db),
) -> TransactionListResponse:
    """List transactions. All filters are optional and AND-composed."""

    # ---- Validate sort field against whitelist -----------------------------
    # Done here (not in the service) so we can return a well-formed 422 with
    # the allowed values listed, and so the service doesn't know about HTTP.
    sort_field_name = sort.lstrip("-")
    if sort_field_name not in SORT_FIELDS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Invalid sort field '{sort_field_name}'. "
                f"Allowed: {', '.join(SORT_FIELDS.keys())}"
            ),
        )

    # ---- Validate date range -----------------------------------------------
    if date_from is not None and date_to is not None and date_from > date_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="date_from must be <= date_to",
        )

    filters = TransactionListFilters(
        merchant_id=merchant_id,
        status=status_filter,
        date_from=date_from,
        date_to=date_to,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    return transaction_query.list_transactions(db, filters)
