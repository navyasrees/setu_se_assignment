"""Reconciliation read queries.

Same principle as transaction_query: read the materialized `transactions` table.
Never re-aggregate over the raw events log at request time.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Set, Tuple
from uuid import UUID

from sqlalchemy import Date, cast, func
from sqlalchemy.orm import Session

from app.models.event import Event, EventType
from app.models.merchant import Merchant
from app.models.transaction import Transaction, TransactionStatus
from app.schemas.reconciliation import (
    DiscrepancyFilters,
    DiscrepancyItem,
    DiscrepancySummary,
    DiscrepancyType,
    ReconciliationDiscrepanciesResponse,
    ReconciliationSummaryResponse,
    StatusBreakdown,
    SummaryFilters,
    SummaryGroup,
    SummaryTotals,
)


def summary(
    db: Session, filters: SummaryFilters
) -> ReconciliationSummaryResponse:
    """Return transactions grouped by (merchant × UTC date × status) plus totals.

    One SQL round-trip drives the groups. Totals are computed from the groups
    in Python — cheaper than a second aggregate query, and consistent by
    construction (totals can't drift from groups because they're derived from
    the same rows).
    """

    # ---- Group query --------------------------------------------------------
    # DATE() in Postgres truncates a timestamptz to a calendar date in the
    # session's timezone. Cast to Date explicitly so the output is a Python
    # date object, which Pydantic serializes as "YYYY-MM-DD". We don't force a
    # timezone here — the app's Postgres session defaults to UTC (confirm in
    # your deployment) so buckets align on UTC-day boundaries.
    date_bucket = cast(Transaction.initiated_at, Date).label("date")

    q = (
        db.query(
            Transaction.merchant_id,
            Merchant.name.label("merchant_name"),
            date_bucket,
            Transaction.current_status,
            func.count().label("count"),
            func.sum(Transaction.amount).label("total_amount"),
        )
        .join(Merchant, Transaction.merchant_id == Merchant.merchant_id)
        # initiated_at should always be set for rows created via ingestion, but
        # be defensive — NULL would blow up the DATE cast and break grouping.
        .filter(Transaction.initiated_at.isnot(None))
    )

    if filters.merchant_id is not None:
        q = q.filter(Transaction.merchant_id == filters.merchant_id)
    if filters.status is not None:
        q = q.filter(Transaction.current_status == filters.status)
    if filters.date_from is not None:
        q = q.filter(Transaction.initiated_at >= filters.date_from)
    if filters.date_to is not None:
        q = q.filter(Transaction.initiated_at <= filters.date_to)

    q = q.group_by(
        Transaction.merchant_id,
        Merchant.name,
        date_bucket,
        Transaction.current_status,
    ).order_by(
        date_bucket.desc(),
        Transaction.merchant_id,
        Transaction.current_status,
    )

    rows = q.all()

    groups = [
        SummaryGroup(
            merchant_id=r.merchant_id,
            merchant_name=r.merchant_name,
            date=r.date,
            status=r.current_status,
            count=r.count,
            total_amount=r.total_amount or Decimal("0"),
        )
        for r in rows
    ]

    # ---- Totals (derived from groups) ---------------------------------------
    # Start every status at zero so the response shape is predictable even when
    # a given status has no rows in the filtered set.
    by_status: dict[TransactionStatus, StatusBreakdown] = {
        s: StatusBreakdown(count=0, total_amount=Decimal("0"))
        for s in TransactionStatus
    }
    # If the caller filtered down to one status, only include that one — don't
    # mislead the client with three zero rows.
    if filters.status is not None:
        by_status = {filters.status: by_status[filters.status]}

    total_count = 0
    total_amount = Decimal("0")

    for g in groups:
        total_count += g.count
        total_amount += g.total_amount
        bucket = by_status.get(g.status)
        if bucket is not None:
            bucket.count += g.count
            bucket.total_amount += g.total_amount

    totals = SummaryTotals(
        transaction_count=total_count,
        total_amount=total_amount,
        by_status=by_status,
    )

    return ReconciliationSummaryResponse(
        filters=filters,
        totals=totals,
        groups=groups,
    )


# ---------------------------------------------------------------------------
#   Discrepancy detection
# ---------------------------------------------------------------------------


def _conflicting_event_txn_ids(
    db: Session, merchant_id: Optional[str]
) -> Set[UUID]:
    """Transactions whose event log contains BOTH a payment_failed AND a settled
    event. These two events are terminal and mutually exclusive by definition,
    so co-occurrence means the source system is sending contradictory signals
    — a classic reconciliation flag.

    Uses GROUP BY + HAVING so Postgres does the heavy lifting; we only ship the
    transaction_ids back across the wire.
    """
    q = (
        db.query(Event.transaction_id)
        .filter(
            Event.event_type.in_(
                [EventType.PAYMENT_FAILED, EventType.SETTLED]
            )
        )
        .group_by(Event.transaction_id)
        .having(func.count(func.distinct(Event.event_type)) == 2)
    )
    if merchant_id is not None:
        q = q.filter(Event.merchant_id == merchant_id)
    return {row[0] for row in q.all()}


def _stuck_txn_ids(
    db: Session,
    status: TransactionStatus,
    timestamp_col,
    older_than: datetime,
    merchant_id: Optional[str],
) -> Set[UUID]:
    """Transactions sitting in `status` past the staleness threshold."""
    q = db.query(Transaction.transaction_id).filter(
        Transaction.current_status == status,
        timestamp_col.isnot(None),
        timestamp_col < older_than,
    )
    if merchant_id is not None:
        q = q.filter(Transaction.merchant_id == merchant_id)
    return {row[0] for row in q.all()}


def _hydrate(
    db: Session, txn_ids: Iterable[UUID]
) -> dict[UUID, tuple[Transaction, str]]:
    """Bulk-fetch (transaction, merchant_name) pairs for a set of IDs.

    One round-trip instead of N — cheaper than lazy-loading per item.
    """
    txn_ids = list(txn_ids)
    if not txn_ids:
        return {}

    rows = (
        db.query(Transaction, Merchant.name.label("merchant_name"))
        .join(Merchant, Transaction.merchant_id == Merchant.merchant_id)
        .filter(Transaction.transaction_id.in_(txn_ids))
        .all()
    )
    return {txn.transaction_id: (txn, name) for txn, name in rows}


_DETAIL_TEMPLATES = {
    DiscrepancyType.CONFLICTING_EVENTS: (
        "Both payment_failed and settled events exist for this transaction"
    ),
    DiscrepancyType.STUCK_IN_PROCESSED: (
        "Processed but not settled for more than {hours} hour(s)"
    ),
    DiscrepancyType.STUCK_IN_INITIATED: (
        "Initiated but not progressed for more than {hours} hour(s)"
    ),
}


def _build_items(
    kind: DiscrepancyType,
    txn_ids: Set[UUID],
    hydrated: dict[UUID, tuple[Transaction, str]],
    hours: Optional[int] = None,
) -> List[DiscrepancyItem]:
    """Turn a set of transaction_ids into DiscrepancyItems of one kind.

    `hydrated` is the shared lookup built from one bulk query — we pass it in
    rather than re-querying per discrepancy type.
    """
    template = _DETAIL_TEMPLATES[kind]
    detail = template.format(hours=hours) if hours is not None else template

    items: List[DiscrepancyItem] = []
    for txn_id in txn_ids:
        pair = hydrated.get(txn_id)
        if pair is None:
            # Shouldn't happen if the detectors are wired correctly, but guard
            # against races where a row is deleted between the detector query
            # and the hydrate query.
            continue
        txn, merchant_name = pair
        items.append(
            DiscrepancyItem(
                type=kind,
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
                detail=detail,
            )
        )
    return items


def discrepancies(
    db: Session, filters: DiscrepancyFilters
) -> ReconciliationDiscrepanciesResponse:
    """Detect and return transactions in inconsistent or stuck states.

    Strategy:
      1. Run a detector per discrepancy type — each returns a set of txn_ids.
      2. Union the IDs and hydrate the full transaction rows in one query.
      3. Build DiscrepancyItems grouped by type.

    A transaction can legitimately be flagged by multiple detectors (e.g. a
    conflicting_events row that's also stuck_in_processed). We emit one
    DiscrepancyItem per (type, transaction) so the client gets full context
    without deduping on their side.
    """

    now = datetime.now(timezone.utc)
    processed_threshold = now - timedelta(hours=filters.processed_stale_hours)
    initiated_threshold = now - timedelta(hours=filters.initiated_stale_hours)

    # ---- Which detectors to run --------------------------------------------
    # If the caller asked for one type, skip the others entirely. Saves SQL.
    run_conflict = filters.type in (None, DiscrepancyType.CONFLICTING_EVENTS)
    run_stuck_p = filters.type in (None, DiscrepancyType.STUCK_IN_PROCESSED)
    run_stuck_i = filters.type in (None, DiscrepancyType.STUCK_IN_INITIATED)

    conflict_ids: Set[UUID] = (
        _conflicting_event_txn_ids(db, filters.merchant_id)
        if run_conflict
        else set()
    )
    stuck_processed_ids: Set[UUID] = (
        _stuck_txn_ids(
            db,
            TransactionStatus.PROCESSED,
            Transaction.processed_at,
            processed_threshold,
            filters.merchant_id,
        )
        if run_stuck_p
        else set()
    )
    stuck_initiated_ids: Set[UUID] = (
        _stuck_txn_ids(
            db,
            TransactionStatus.INITIATED,
            Transaction.initiated_at,
            initiated_threshold,
            filters.merchant_id,
        )
        if run_stuck_i
        else set()
    )

    # ---- Hydrate all flagged rows in one round-trip -------------------------
    all_flagged = conflict_ids | stuck_processed_ids | stuck_initiated_ids
    hydrated = _hydrate(db, all_flagged)

    # ---- Build response items -----------------------------------------------
    items: List[DiscrepancyItem] = []
    items.extend(_build_items(DiscrepancyType.CONFLICTING_EVENTS, conflict_ids, hydrated))
    items.extend(
        _build_items(
            DiscrepancyType.STUCK_IN_PROCESSED,
            stuck_processed_ids,
            hydrated,
            hours=filters.processed_stale_hours,
        )
    )
    items.extend(
        _build_items(
            DiscrepancyType.STUCK_IN_INITIATED,
            stuck_initiated_ids,
            hydrated,
            hours=filters.initiated_stale_hours,
        )
    )

    # Stable ordering: by type, then by transaction_id. Makes the response
    # diffable across calls and predictable for screenshots.
    items.sort(key=lambda d: (d.type.value, str(d.transaction_id)))

    # ---- Summary counts -----------------------------------------------------
    by_type: dict[DiscrepancyType, int] = {
        DiscrepancyType.CONFLICTING_EVENTS: len(conflict_ids),
        DiscrepancyType.STUCK_IN_PROCESSED: len(stuck_processed_ids),
        DiscrepancyType.STUCK_IN_INITIATED: len(stuck_initiated_ids),
    }
    if filters.type is not None:
        by_type = {filters.type: by_type[filters.type]}

    return ReconciliationDiscrepanciesResponse(
        filters=filters,
        summary=DiscrepancySummary(total=len(items), by_type=by_type),
        discrepancies=items,
    )
