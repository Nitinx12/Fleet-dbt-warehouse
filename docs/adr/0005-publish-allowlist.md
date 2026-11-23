Purpose: Record why publication uses an explicit mart allowlist.
Audience: Contributors adding marts or changing the Databricks publish job.
[Root README](../../README.md)

# 0005 — Publish allowlist

## Context
Six BI marts exist in Postgres. The driver scorecard carries a driver display name and must not be published; publishing every `serve: true` model would also expose unreviewed grains. The Databricks copy is for read-only BI.

## Decision
The publisher writes only five marts: `mart_sales_daily`, `mart_equipment_utilization_daily`, `mart_delivery_performance_daily`, `mart_customer_service_monthly`, and `mart_truck_cost_monthly`. `mart_driver_scorecard_monthly` stays in Postgres, and `assert_marts_reconcile_to_facts` is not a mart.

## Consequences
- PII-tagged and unreviewed models cannot reach Databricks through the publish job.
- A new BI grain needs a reviewed PR that changes the allowlist.
- The publisher validates `mart_key` and `mart_loaded_at`, blocks target schema drift, merges on `mart_key`, verifies row counts, and restores the prior Delta version on failure.
