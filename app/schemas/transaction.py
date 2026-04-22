"""Pydantic schemas for the /transactions endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.models.transaction import TransactionStatus


class TransactionListItem(BaseModel):
    """One row in the paginated transaction list."""

    transaction_id: UUID
    merchant_id: str
    merchant_name: str
    amount: Decimal
    currency: str
    current_status: TransactionStatus

    # All four lifecycle timestamps, even when null — keeps the response shape
    # stable so clients can render a timeline without branching on existence.
    initiated_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    settled_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class Pagination(BaseModel):
    total: int
    limit: int
    offset: int


class TransactionListResponse(BaseModel):
    items: List[TransactionListItem]
    pagination: Pagination


class TransactionListFilters(BaseModel):
    """Internal representation of filter/sort/paginate options.

    This is not used directly as a request schema — the router accepts individual
    Query() parameters and packs them into this model before passing to the
    service. That keeps the router's OpenAPI docs clean while giving the service
    a single, typed argument.
    """

    merchant_id: Optional[str] = None
    status: Optional[TransactionStatus] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    sort: str = "-initiated_at"
    limit: int = 50
    offset: int = 0
