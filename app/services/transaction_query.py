"""Transaction read queries.

Everything here reads the derived `transactions` table directly. We never
aggregate over the raw events log at request time — that's what the ingestion
service maintains the materialized state for.
"""

from typing import Optional
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.event import Event
from app.models.merchant import Merchant
from app.models.transaction import Transaction
from app.schemas.transaction import (
    EventHistoryItem,
    Pagination,
    TransactionDetailResponse,
    TransactionListFilters,
    TransactionListItem,
    TransactionListResponse,
)


# Whitelist of sortable columns. Keeping this explicit serves two purposes:
# (1) it prevents the client from ordering by arbitrary columns, which would
# leak implementation details; (2) it maps user-facing names to SQLAlchemy
# column expressions without any string interpolation.
SORT_FIELDS = {
    "initiated_at": Transaction.initiated_at,
    "amount": Transaction.amount,
    "current_status": Transaction.current_status,
    "updated_at": Transaction.updated_at,
}


def list_transactions(
    db: Session, filters: TransactionListFilters
) -> TransactionListResponse:
    """Return a paginated, filtered, sorted list of transactions.

    The query relies on these indexes:
      - idx_txn_merchant_status   (merchant_id, current_status)
      - idx_txn_initiated_at      (initiated_at)
      - merchants PK              (for the join)
    """

    # Join on merchants so every row carries merchant_name. Saves the client a
    # separate round-trip, and the join is cheap (5 merchant rows in total).
    query = db.query(Transaction, Merchant.name.label("merchant_name")).join(
        Merchant, Transaction.merchant_id == Merchant.merchant_id
    )

    # ---- Filters (AND-composed; only applied when caller provided them) -----
    if filters.merchant_id is not None:
        query = query.filter(Transaction.merchant_id == filters.merchant_id)
    if filters.status is not None:
        query = query.filter(Transaction.current_status == filters.status)
    if filters.date_from is not None:
        query = query.filter(Transaction.initiated_at >= filters.date_from)
    if filters.date_to is not None:
        query = query.filter(Transaction.initiated_at <= filters.date_to)

    # ---- Total count, before pagination --------------------------------------
    # One extra SQL round-trip, but the small derived table makes it cheap and
    # lets the client render "page X of N" without a second request.
    total = query.count()

    # ---- Sort ---------------------------------------------------------------
    # filters.sort format: "-field" for DESC, "field" for ASC. Field has already
    # been validated against SORT_FIELDS at the router layer.
    is_desc = filters.sort.startswith("-")
    field_name = filters.sort.lstrip("-")
    column = SORT_FIELDS[field_name]
    order = column.desc() if is_desc else column.asc()

    # Secondary sort on transaction_id for stable pagination: without it, rows
    # with identical sort-key values can reorder between pages and cause
    # duplicates or skips in what the client sees.
    rows = (
        query.order_by(order, Transaction.transaction_id)
        .limit(filters.limit)
        .offset(filters.offset)
        .all()
    )

    items = [
        TransactionListItem(
            transaction_id=txn.transaction_id,
            merchant_id=txn.merchant_id,
            merchant_name=merchant_name,
            amount=txn.amount,
            currency=txn.currency,
            current_status=txn.current_status,
            initiated_at=txn.initiated_at,
            processed_at=txn.processed_at,
            failed_at=txn.failed_at,
            settled_at=txn.settled_at,
        )
        for txn, merchant_name in rows
    ]

    return TransactionListResponse(
        items=items,
        pagination=Pagination(
            total=total, limit=filters.limit, offset=filters.offset
        ),
    )


def get_transaction_detail(
    db: Session, transaction_id: UUID
) -> Optional[TransactionDetailResponse]:
    """Return one transaction's current state plus its full event history.

    Returns None if the transaction doesn't exist — the router translates that
    into a 404. Keeping HTTP concerns out of the service means this function
    stays usable from non-HTTP callers (scripts, tests, background jobs).

    Two queries are issued:
      1. transactions JOIN merchants, keyed by PK          (one row)
      2. events WHERE transaction_id = :id ORDER BY time   (N rows)

    Both are index-backed (txns PK; idx_events_transaction). Keeping them
    separate is simpler than a single LEFT JOIN that would duplicate the
    transaction row once per event and need manual de-duplication in Python.
    """

    row = (
        db.query(Transaction, Merchant.name.label("merchant_name"))
        .join(Merchant, Transaction.merchant_id == Merchant.merchant_id)
        .filter(Transaction.transaction_id == transaction_id)
        .one_or_none()
    )
    if row is None:
        return None

    txn, merchant_name = row

    # Ordered oldest-first. Secondary sort on received_at gives deterministic
    # ordering when two events share an event_timestamp — otherwise rows with
    # identical timestamps could flip between requests.
    events = (
        db.query(Event)
        .filter(Event.transaction_id == transaction_id)
        .order_by(Event.event_timestamp.asc(), Event.received_at.asc())
        .all()
    )

    return TransactionDetailResponse(
        transaction=TransactionListItem(
            transaction_id=txn.transaction_id,
            merchant_id=txn.merchant_id,
            merchant_name=merchant_name,
            amount=txn.amount,
            currency=txn.currency,
            current_status=txn.current_status,
            initiated_at=txn.initiated_at,
            processed_at=txn.processed_at,
            failed_at=txn.failed_at,
            settled_at=txn.settled_at,
        ),
        events=[EventHistoryItem.model_validate(e) for e in events],
    )
