"""Event-ingestion business logic.

The route handler is intentionally thin; all correctness-critical behaviour
lives here so it can be unit-tested without spinning up HTTP.

Correctness properties this module guarantees:

1. Idempotency — the same event_id submitted twice is a no-op on the second call.
2. Order-independence — events for the same transaction can arrive in any order
   and the resulting current_status is the same.
3. Atomicity — the event insert and the transaction state update either both
   commit or both roll back.
"""

from typing import Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models.event import Event, EventType
from app.models.merchant import Merchant
from app.models.transaction import Transaction, TransactionStatus
from app.schemas.event import EventIngestRequest, EventIngestResponse


# Map each event_type to the transactions column we populate on arrival.
_EVENT_TYPE_TO_TIMESTAMP_COL = {
    EventType.PAYMENT_INITIATED: "initiated_at",
    EventType.PAYMENT_PROCESSED: "processed_at",
    EventType.PAYMENT_FAILED: "failed_at",
    EventType.SETTLED: "settled_at",
}


def _compute_status(txn: Transaction) -> TransactionStatus:
    """Derive current_status from which lifecycle timestamps are populated.

    Precedence (settled > failed > processed > initiated) is what makes this
    order-independent: a late-arriving 'initiated' event won't regress a
    transaction that's already settled.
    """
    if txn.settled_at is not None:
        return TransactionStatus.SETTLED
    if txn.failed_at is not None:
        return TransactionStatus.FAILED
    if txn.processed_at is not None:
        return TransactionStatus.PROCESSED
    return TransactionStatus.INITIATED


def _apply_event_to_transaction(txn: Transaction, payload: EventIngestRequest) -> None:
    """Populate the matching *_at timestamp (first-write-wins) and recompute status."""
    column = _EVENT_TYPE_TO_TIMESTAMP_COL[payload.event_type]
    if getattr(txn, column) is None:
        setattr(txn, column, payload.timestamp)
    txn.current_status = _compute_status(txn)


def ingest(db: Session, payload: EventIngestRequest) -> EventIngestResponse:
    """Ingest one event. Safe to call repeatedly with the same payload."""

    # ---- Step 1: Ensure the merchant exists -------------------------------
    # On first event for a new merchant we need a row before the events FK
    # will accept the insert. ON CONFLICT DO NOTHING makes this safe under
    # concurrent ingestion of different events for the same new merchant.
    merchant_stmt = (
        pg_insert(Merchant)
        .values(
            merchant_id=payload.merchant_id,
            name=payload.merchant_name,
        )
        .on_conflict_do_nothing(index_elements=["merchant_id"])
    )
    db.execute(merchant_stmt)

    # ---- Step 2: Idempotently insert the event ----------------------------
    # PK conflict on event_id means we've seen this event before — short-circuit.
    event_values = {
        "event_id": payload.event_id,
        "transaction_id": payload.transaction_id,
        "merchant_id": payload.merchant_id,
        "event_type": payload.event_type,
        "amount": payload.amount,
        "currency": payload.currency,
        "event_timestamp": payload.timestamp,
        "raw_payload": payload.model_dump(mode="json"),
    }
    event_stmt = (
        pg_insert(Event)
        .values(**event_values)
        .on_conflict_do_nothing(index_elements=["event_id"])
    )
    result = db.execute(event_stmt)

    if result.rowcount == 0:
        # Duplicate event. State has already been applied on the original
        # ingestion; don't touch the transactions table.
        db.commit()
        return EventIngestResponse(
            status="duplicate",
            event_id=payload.event_id,
            transaction_id=payload.transaction_id,
            current_status=None,
        )

    # ---- Step 3: Fetch or create the transaction row ----------------------
    txn: Optional[Transaction] = (
        db.query(Transaction)
        .filter(Transaction.transaction_id == payload.transaction_id)
        .with_for_update()  
        .one_or_none()
    )
    if txn is None:
        # First event we've seen for this transaction. Seed a row; current_status
        # is a placeholder that _apply_event_to_transaction overwrites immediately.
        txn = Transaction(
            transaction_id=payload.transaction_id,
            merchant_id=payload.merchant_id,
            amount=payload.amount,
            currency=payload.currency,
            current_status=TransactionStatus.INITIATED,
        )
        db.add(txn)

    # ---- Step 4: Apply event to derived state ----------------------------
    _apply_event_to_transaction(txn, payload)

    # ---- Step 5: Atomic commit -------------------------------------------
    db.commit()
    db.refresh(txn)

    return EventIngestResponse(
        status="accepted",
        event_id=payload.event_id,
        transaction_id=payload.transaction_id,
        current_status=txn.current_status,
    )
