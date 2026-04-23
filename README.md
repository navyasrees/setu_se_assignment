# Setu — Payment Reconciliation Service

A FastAPI service that ingests payment-lifecycle events, materializes the
current state of every transaction, and exposes reconciliation views over
that state. Built for the Setu Solutions Engineer take-home.

The design goal was a system that stays **correct under duplicate events,
out-of-order arrivals, and concurrent writes on the same transaction** — and
keeps an audit trail you can point at when something looks off.

---

## Quick start

### Prerequisites
- Python 3.9+
- Docker (for Postgres)
- `jq` optional, for pretty-printing curl output

### 1. Start Postgres

```bash
docker run -d --name setu-pg \
  -e POSTGRES_PASSWORD=setu -e POSTGRES_DB=setu \
  -p 5432:5432 postgres:16
```

### 2. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure

Copy `.env.example` to `.env` — the defaults work against the Postgres
container above.

```
DATABASE_URL=postgresql+psycopg2://postgres:setu@localhost:5432/setu
```

### 4. Run the server

```bash
uvicorn app.main:app --reload
```

Health check: `curl http://127.0.0.1:8000/health`

### 5. Load the sample data

```bash
python -m scripts.load_sample_data
```

Expects `~/Downloads/sample_events.json`. Reports accepted / duplicate counts
at the end. For the supplied sample, 10,165 accepted and 190 duplicates
(idempotent replays) — exactly matching the known distribution.

### 6. Explore

- Interactive docs: http://127.0.0.1:8000/docs
- REST Client file: `api.rest` (one block per endpoint, click "Send Request")
- Raw curl commands: see the **API** section below

---

## Architecture

### Three tables

```
merchants          events                         transactions
----------         -----------------------        -----------------------
merchant_id  <---  merchant_id                    merchant_id
name               transaction_id       <.....    transaction_id  (PK)
created_at         event_id       (PK)            amount
                   event_type                     currency
                   amount                         current_status
                   currency                       initiated_at
                   event_timestamp                processed_at
                   received_at                    failed_at
                   raw_payload (JSONB)            settled_at
                                                  created_at / updated_at
```

- **`events`** is append-only. Every incoming webhook becomes one row. The
  `event_id` is the primary key, which is what gives us idempotency for
  free — replays collide on the PK and we turn that into a clean
  "duplicate" response.

- **`transactions`** is the materialized current-state table. It's updated
  inside the same DB transaction as the event insert, so the two never
  drift. Read endpoints hit this table directly — we never re-aggregate
  over the raw event log at request time.

- **`merchants`** is a reference table — five rows, small, stable. Joined on
  list/detail responses so the client gets `merchant_name` without a
  separate round-trip.

### Layering

```
app/
├── main.py              — wiring; thin
├── core/config.py       — settings via pydantic-settings (.env)
├── db/                  — SQLAlchemy engine, session, Base
├── models/              — ORM tables (Merchant, Event, Transaction)
├── schemas/             — Pydantic request/response models
├── services/            — domain logic; routers call these
│   ├── event_ingestion.py
│   ├── transaction_query.py
│   └── reconciliation.py
└── routers/             — HTTP surface only; no business logic
    ├── events.py
    ├── transactions.py
    └── reconciliation.py
```

**Services never import FastAPI.** That means the domain logic is testable
without an HTTP client, and reusable from scripts (see
`scripts/load_sample_data.py`, which calls `event_ingestion.ingest()`
directly against a DB session).

---

## Ingestion correctness

`POST /events` guarantees three properties on every call:

### 1. Idempotency — same `event_id` twice is a no-op

The `events.event_id` column is the primary key. The insert uses
Postgres's `INSERT ... ON CONFLICT (event_id) DO NOTHING`. If the insert
affected zero rows, we know this was a replay and short-circuit with a
`200 OK { status: "duplicate" }` — no transactions-table mutation, no
duplicate event.

```json
First call  →  201 Created  { "status": "accepted",  "current_status": "initiated" }
Replay      →  200 OK       { "status": "duplicate", "current_status": "initiated" }
```

### 2. Order-independence — late events can't regress state

Events can arrive out of order (network retries, queue replays, multi-region
lag). A late-arriving `payment_initiated` for a transaction that's already
`settled` must not flip the status back.

Enforced with a precedence rule:

```
settled (3)  >  failed (2)  >  processed (1)  >  initiated (0)
```

On every event, we compute `new_status = max(old_status, event_status)`
using that precedence. A late event with a lower-precedence status is
recorded in the event log (we keep the history), but doesn't touch the
`current_status` column.

Per-event-type timestamps (`initiated_at`, `processed_at`, `failed_at`,
`settled_at`) use **first-write-wins**: if a timestamp is already set,
subsequent events of the same type don't overwrite it. This preserves the
"when did we first see this?" signal.

### 3. Atomicity — event + state update are one database transaction

Every ingest request runs inside a single DB transaction:

```
BEGIN
  INSERT INTO events ... ON CONFLICT DO NOTHING
  SELECT ... FROM transactions WHERE transaction_id = :id FOR UPDATE
  INSERT OR UPDATE transactions ...
COMMIT
```

If anything in that block fails, the whole thing rolls back. You can
never land a row in `events` without the corresponding `transactions`
update, or vice versa.

---

## Concurrency

Two races to reason about:

### Race 1 — same `event_id`, two concurrent requests

Worker A and worker B both process the same event at the same moment.
Both try to `INSERT INTO events`. **The database serializes them**: one
succeeds, the other hits the PK conflict and `DO NOTHING`. Both return
clean responses; no double-count.

### Race 2 — different `event_id`s, same `transaction_id`

Worker A ingests `settled` for txn X; worker B ingests `payment_failed`
for txn X. Both inserts succeed in the `events` table (different PKs).
The danger is two concurrent read-modify-writes of the same
`transactions` row interleaving.

Mitigated with `SELECT ... FOR UPDATE` — a pessimistic row lock on the
transaction row before we read `current_status`. The second worker blocks
until the first commits, then reads the committed state and merges
correctly.

### The production answer: partitioned queue

Locks work, but they serialize writes and cost throughput. In production
I'd put a partitioned queue (Kafka / SQS FIFO / Redis Streams) in front of
the ingest service, partitioned on `transaction_id`. Events for the same
transaction land in the same partition, consumed by a single worker, and
the race disappears at the architecture level — no locks needed.

The current implementation is correct without the queue; the queue is a
scale-out optimization.

---

## API reference

Base URL: `http://127.0.0.1:8000`

### `POST /events` — ingest an event

```bash
curl -is -X POST http://127.0.0.1:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "...",
    "event_type": "payment_initiated",
    "transaction_id": "...",
    "merchant_id": "merchant_2",
    "merchant_name": "FreshBasket",
    "amount": 15248.29,
    "currency": "INR",
    "timestamp": "2026-01-08T12:11:58+00:00"
  }'
```

- `201 Created` — new event accepted
- `200 OK` — duplicate `event_id` (idempotent replay)
- `422` — validation failure (negative amount, unknown `event_type`, …)

### `GET /transactions` — list with filter / sort / paginate

Query params: `merchant_id`, `status`, `date_from`, `date_to`, `sort`, `limit`, `offset`.

`sort` accepts: `initiated_at`, `amount`, `current_status`, `updated_at`
(prefix with `-` for descending). Stable pagination uses a secondary
sort on `transaction_id`.

```bash
curl -s 'http://127.0.0.1:8000/transactions?merchant_id=merchant_2&status=settled&sort=-amount&limit=10'
```

### `GET /transactions/{id}` — detail + full event history

Returns the materialized transaction row **plus** every event we've
received for it, oldest-first. Proof of *why* the state is what it is.

```bash
curl -s http://127.0.0.1:8000/transactions/<uuid>
```

- `404` — unknown transaction
- `422` — malformed UUID

### `GET /reconciliation/summary` — aggregates grouped by merchant × UTC date × status

Returns filter-wide totals + per-group counts and sums. No pagination —
bounded by merchants × days × 4 statuses.

```bash
curl -s 'http://127.0.0.1:8000/reconciliation/summary?merchant_id=merchant_2'
```

### `GET /reconciliation/discrepancies` — inconsistent / stuck transactions

Three detectors:

| Type | Definition |
|---|---|
| `conflicting_events` | Events log has both a `payment_failed` AND a `settled` event for the same transaction |
| `stuck_in_processed` | `current_status='processed'` and `processed_at` older than `processed_stale_hours` (default 24) |
| `stuck_in_initiated` | `current_status='initiated'` and `initiated_at` older than `initiated_stale_hours` (default 1) |

```bash
curl -s 'http://127.0.0.1:8000/reconciliation/discrepancies?type=conflicting_events'
```

---

## Indexes

| Table | Index | Covers |
|---|---|---|
| `events` | PK on `event_id` | Idempotency check |
| `events` | `(transaction_id, event_timestamp)` | Per-transaction history in `/transactions/{id}` |
| `events` | `(merchant_id, event_timestamp)` | Future merchant-scoped event queries |
| `events` | `(event_type, event_timestamp)` | Conflicting-events detector |
| `transactions` | PK on `transaction_id` | Detail lookup |
| `transactions` | `(merchant_id, current_status)` | Filter by merchant and/or status |
| `transactions` | `(initiated_at)` | Date-range filter; summary grouping |
| `transactions` | `(current_status, updated_at)` | Stuck-state detectors |

---

## Tradeoffs / what I'd add next

- **Alembic migrations** — currently `Base.metadata.create_all()` on startup (fine for dev, not prod).
- **Structured errors** — standardize on `{code, message, details}` across 4xx responses.
- **Cursor pagination** — offset pagination is O(n) on deep pages. For a real dashboard, switch to a keyset/cursor scheme on `(initiated_at, transaction_id)`.
- **Observability** — request/response logging middleware, Prometheus metrics on ingest throughput and per-status counts, structured logs with `transaction_id` correlation.
- **Auth** — API key or JWT on every route. Rate limiting on `/events`.
- **Outbox pattern** — if ingestion needs to fan out to downstream systems (notifications, ledger), write an outbox row in the same transaction as the event insert and have a separate poller publish it.
- **Partitioned ingest queue** — see Concurrency section. The scale-out path.
- **Test suite** — unit tests for `_compute_status` precedence; integration tests against a test DB with the race scenarios; property-based tests on idempotency.

---

## Project layout

```
setu_se_assignment/
├── README.md                  — this file
├── api.rest                   — VS Code REST Client test blocks
├── requirements.txt
├── .env.example
├── app/                       — application code
├── scripts/
│   └── load_sample_data.py    — bulk-ingest sample_events.json
└── ASSIGNMENT.md              — the original brief (for reference)
```
