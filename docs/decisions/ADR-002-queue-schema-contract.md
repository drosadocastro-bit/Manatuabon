# ADR-002: Queue Schema Contract

## Status
Accepted

## Date
2026-04-11

## Context
Manatuabon uses three queue tables (`mast_queue`, `radio_queue`, `simulations`)
for different workers. These were created independently over multiple phases:

- `mast_queue` had `attempts` and `last_run` columns; others didn't
- `simulations` used `status='completed'` while others used `status='done'`
- `radio_queue` lacked retry columns entirely

This caused the reject endpoint bug (wrong status string) and made it impossible
to write a generic `BaseWorker` that works across all queues.

## Decision
All queue tables must conform to a shared schema contract:

**Required columns for any queue table:**
| Column | Type | Purpose |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | Unique task identifier |
| `status` | TEXT | One of: `pending`, `running`, `done`, `failed` |
| `attempts` | INTEGER DEFAULT 0 | Retry counter |
| `last_run` | TEXT | ISO timestamp of last execution |
| `queued_at` | TEXT | ISO timestamp of when task was created |

**Status enum (all queues):**
- `pending` — waiting to be processed
- `running` — currently being processed
- `done` — completed successfully
- `failed` — failed (may be retried if attempts < max)

No other status values are permitted in queue tables. Hypothesis statuses
(accepted, held, rejected, etc.) are a separate domain governed by GOVERNANCE.md §6.

## Alternatives Considered

### Per-worker custom schemas
- Pros: Each worker gets exactly what it needs
- Cons: No shared BaseWorker possible, inconsistencies accumulate silently
- Rejected: Already caused bugs; consistency is worth the constraint

### NoSQL / JSON columns for flexible schema
- Pros: No migration needed, each worker stores what it wants
- Cons: No type safety, no shared contract, harder to query
- Rejected: SQLite's flexibility is sufficient; structure prevents drift

## Consequences
- All queue workers can share a `BaseWorker` class
- Schema migrations in `db_init.py` enforce the contract
- New workers must include all required columns
- Status constants should be defined once and imported (not inline strings)

## Governance Impact
None — queue schema is operational infrastructure, not governance-sensitive.
