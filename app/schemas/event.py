"""Pydantic request/response models for the /events endpoint.

These are the API-facing shapes — intentionally separate from the SQLAlchemy
models in app/models/event.py. The Pydantic model validates input; the SQLAlchemy
model represents a database row. Never confuse the two.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.models.event import EventType
from app.models.transaction import TransactionStatus


class EventIngestRequest(BaseModel):
    """Matches the shape of each object in sample_events.json."""

    event_id: UUID
    event_type: EventType
    transaction_id: UUID
    merchant_id: str = Field(..., max_length=50)
    merchant_name: str = Field(..., max_length=255)
    amount: Decimal = Field(..., gt=0, max_digits=14, decimal_places=2)
    currency: str = Field(default="INR", min_length=3, max_length=3)
    timestamp: datetime

    model_config = ConfigDict(use_enum_values=False)


class EventIngestResponse(BaseModel):
    """Returned from POST /events.

    status is 'accepted' for a new event, 'duplicate' if we'd already ingested
    this event_id. current_status is null in the duplicate case because we don't
    re-read the transaction row on a short-circuit.
    """

    status: str
    event_id: UUID
    transaction_id: UUID
    current_status: Optional[TransactionStatus] = None
