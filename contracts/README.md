# Source contracts

**Purpose:** Define the source-to-bronze contract format and the current alignment check.

**Audience:** Engineers changing source contracts, ingestion, or bronze sources.

[Root README](../README.md)

Versioned agreements at the **source → bronze** boundary. One YAML per table/collection, plus a JSON sample of real rows.

| Path | Purpose |
|---|---|
| `sources/*.yml` | The contract: columns, types, PK, FKs, extraction, quality, freshness, change policy |
| `samples/*.sample.json` | 3 real rows (Mongo: raw strings + staged `try_cast` preview) |
| `sources_index.json` | Machine index of all contracts |

## Format

See [ARCHITECTURE.md](../ARCHITECTURE.md) for the source-contract boundary.

- **MySQL** — `type` is the landed type in Postgres `bronze.mysql__*`.
- **Mongo** — every raw value is a string; `staged_type` + `cast` describe what staging produces (`try_cast`, `flag`).

## Commands

```bash
make contracts-check    # fail on index / source name / landing table name drift
```

`contracts-check` validates `sources_index.json` against the contract files, the `source` value in each YAML, and the bronze landing table names declared in `dbt/models/bronze/sources.yml`. It does not check columns, row-count baselines or JSON samples. The pre-push hook checks that contract JSON parses; CI runs `contracts-check` before ingestion changes.

## Changing a contract

1. Edit the YAML **and** the sample in the same PR.
2. Additive nullable column → `minor_bump` in `change_policy` / minor version.
3. Remove, rename or retype → **breaking**: bump `contract_version` major, note in `CHANGELOG.md`, ask first.
4. Never edit a deployed contract version in place to hide a break.
