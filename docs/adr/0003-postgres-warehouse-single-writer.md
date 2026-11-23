Purpose: Record why each warehouse layer has one writer.
Audience: Contributors adding jobs, SQL, or write paths.
[Root README](../../README.md)

# 0003 — Postgres warehouse, single writer per layer

## Context
Postgres hosts bronze, silver, gold, marts, audit, and control. Multiple services patching the same schemas would make replay and incident triage ambiguous and allow silent cross-layer fixes.

## Decision
Each layer has one writer: ingestion and fixture loading write bronze; dbt writes silver, gold, and marts; `control.*` functions, Python helpers, and Airflow write control; Python helpers and SQL triggers write audit. The publish job alone writes the Databricks mart copy, which is read-only to everything else.

## Consequences
- Replay and incident triage stay deterministic.
- Cross-layer convenience writes are deliberately blocked.
- Writers coordinate through contracts, `control.batch`, and append-only audit evidence rather than shared write access.
