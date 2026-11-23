Purpose: Record why Airflow schedules work but scripts own commands.
Audience: Contributors changing DAGs, scripts, or Make targets.
[Root README](../../README.md)

# 0006 — Airflow schedules; scripts own commands

## Context
The same work runs locally, in CI, and under orchestration. Embedding transformation, connection, or command logic in DAG code would create a third execution path and hide failures from the existing test surface.

## Decision
Airflow's `fleet_daily_load` runs at `0 2 * * *` in `Asia/Kolkata` and sequences preflight, batch, ingestion, dbt, publish, and summary tasks. DAG tasks invoke argv arrays such as `uv run --no-sync python scripts/<name>.py ...` with validated identifiers and no shell interpolation. dbt commands are built as `run` then `test` stages.

## Consequences
- Local, CI, and orchestrated runs use the same scripts and Make targets.
- Schedules are cron and task dependencies; asset keys from the dbt manifest are not wired.
- DAG tests cover structure, tiers, callbacks, redaction, and command builders without moving data.
