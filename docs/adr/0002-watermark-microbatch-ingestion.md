Purpose: Record how source data is landed incrementally and replayed safely.
Audience: Contributors changing ingestion, contracts, watermarks, or lookbacks.
[Root README](../../README.md)

# 0002 — Watermark micro-batch ingestion

## Context
The 12 source tables (9 MySQL, 3 MongoDB) are read with read-only credentials. Full-table nightly replacement would reread everything and lose a bounded change window; source rows can arrive late or change. Ingestion is Python and only moves data into bronze.

## Decision
Contracts define each source's cursor and lookback. `INCREMENTAL` extracts `cursor > lower AND cursor <= upper`, upserts on the contract primary key, and advances the watermark only after the bronze write and reconciliation succeed. `FULL` loads delete then insert and ignore watermarks. Reprocessing relies on watermark windows plus downstream silver and gold lookbacks.

## Consequences
- Re-running the same window upserts the same keys and leaves row values unchanged.
- Late or changed rows are reprocessed without full reloads.
- Watermark resets and reprocessing windows must be monitored.
- Fixture loading uses the same bronze writer but does not run live extraction or advance watermarks.
