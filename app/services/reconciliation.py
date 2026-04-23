"""Reconciliation read queries.

Same principle as transaction_query: read the materialized `transactions` table.
Never re-aggregate over the raw events log at request time.
"""

from decimal import Decimal

from sqlalchemy import Date, cast, func
from sqlalchemy.orm import Session

from app.models.merchant import Merchant
from app.models.transaction import Transaction, TransactionStatus
from app.schemas.reconciliation import (
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
