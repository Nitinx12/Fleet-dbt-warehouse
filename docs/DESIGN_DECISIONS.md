# Design decisions

**Purpose:** Hold the full trade-off table referenced by the README.

**Audience:** Engineers reviewing why the warehouse is shaped this way.

[Back to README](../README.md) · [Architecture](ARCHITECTURE.md)

| Choice | Alternative not used | Trade-off |
|---|---|---|
| dbt owns transformation; Python only moves data | SQL inside ingestion or stored procedures | One testable lineage, with more data landing in `bronze` before it is shaped. |
| Watermark micro-batches with a lookback | Nightly full-table replacement | Handles late and changed rows without full reloads; watermarks and reprocessing windows need monitoring. |
| One writer per layer | Multiple services patching shared schemas | Replay and incident triage stay deterministic; cross-layer convenience writes are blocked. |
| Current-state SCD1 dimensions | SCD2 history on every dimension | Simple, fast BI; historical attribution is not available until snapshots are added. |
| Five-mart publication allowlist | Publish every reviewed mart | Keeps PII and unreviewed models out of Databricks; new BI grains require a reviewed PR. |
