Purpose: Record why dbt is the only transformation engine.
Audience: Contributors changing ingestion, dbt models, or warehouse layers.
[Root README](../../README.md)

# 0001 — dbt owns transformation

## Context
The daily pipeline lands 12 source tables (9 MySQL, 3 MongoDB) into Postgres bronze. dbt builds 32 models across silver, gold, and marts and carries 383 dbt tests. SQL in ingestion, stored procedures, or the publish job would split lineage and test ownership across runtimes.

## Decision
dbt reads bronze through `source()` and writes silver, gold, and marts. Python extracts and lands rows, publishes approved marts, and records audit evidence; it does not change values. Bronze is read-only to dbt, and only the publisher writes the Databricks mart copy.

## Consequences
- One transformation lineage and one primary dbt test surface.
- More raw bytes land in bronze before cleaning.
- Contract alignment at bronze is a prerequisite: `make contracts-check` checks index, contract, and dbt source names.
