"""HTTP layer for event ingestion. Keep this thin — logic lives in the service."""

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.event import EventIngestRequest, EventIngestResponse
from app.services import event_ingestion

router = APIRouter(prefix="/events", tags=["events"])


@router.post(
    "",
    response_model=EventIngestResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest a payment lifecycle event",
)
def ingest_event(
    payload: EventIngestRequest,
    response: Response,
    db: Session = Depends(get_db),
) -> EventIngestResponse:
    """Ingest a single payment lifecycle event.

    Returns 201 when the event is accepted (first time seeing this event_id).
    Returns 200 when the event is a duplicate — the request is well-formed,
    but nothing changed on the server.
    """
    result = event_ingestion.ingest(db, payload)
    if result.status == "duplicate":
        response.status_code = status.HTTP_200_OK
    return result
