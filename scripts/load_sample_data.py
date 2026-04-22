"""Bulk-load sample_events.json into the database.

Runs every event through the same `event_ingestion.ingest()` service function
that `POST /events` uses, so this script also serves as an integration test of
the ingestion pipeline against the full dataset — including the 190 duplicate
event_ids, which should all come back as 'duplicate'.

Usage (from project root):
    python scripts/load_sample_data.py                              # default path
    python scripts/load_sample_data.py path/to/sample_events.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

from pydantic import ValidationError

from app.db.session import SessionLocal
from app.schemas.event import EventIngestRequest
from app.services import event_ingestion


DEFAULT_PATH = Path.home() / "Downloads" / "sample_events.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk-load sample_events.json")
    parser.add_argument(
        "path",
        nargs="?",
        default=str(DEFAULT_PATH),
        help=f"Path to sample_events.json (default: {DEFAULT_PATH})",
    )
    args = parser.parse_args()

    source = Path(args.path).expanduser()
    if not source.exists():
        print(f"ERROR: {source} not found", file=sys.stderr)
        return 1

    with source.open() as f:
        events = json.load(f)

    print(f"Loading {len(events):,} events from {source} ...")
    start = time.time()

    stats: Counter[str] = Counter()
    db = SessionLocal()
    try:
        for idx, raw in enumerate(events, start=1):
            try:
                payload = EventIngestRequest.model_validate(raw)
                result = event_ingestion.ingest(db, payload)
                stats[result.status] += 1
            except ValidationError as e:
                stats["validation_error"] += 1
                print(f"  [{idx}] validation error: {e}", file=sys.stderr)
                db.rollback()
            except Exception as e:
                stats["error"] += 1
                print(f"  [{idx}] error: {e}", file=sys.stderr)
                db.rollback()

            if idx % 1000 == 0:
                elapsed = time.time() - start
                rate = idx / elapsed if elapsed else 0
                print(f"  ...processed {idx:,} events in {elapsed:.1f}s ({rate:.0f}/s)")
    finally:
        db.close()

    elapsed = time.time() - start
    total = sum(stats.values())
    print()
    print(f"Done in {elapsed:.1f}s ({total / elapsed:.0f} events/sec)")
    print(f"  accepted:          {stats['accepted']:>7,}")
    print(f"  duplicate:         {stats['duplicate']:>7,}")
    print(f"  validation_error:  {stats['validation_error']:>7,}")
    print(f"  error:             {stats['error']:>7,}")
    print(f"  total processed:   {total:>7,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
