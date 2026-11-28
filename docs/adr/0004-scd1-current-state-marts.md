Purpose: Record why gold dimensions and marts are current-state SCD1.
Audience: Contributors modelling dimensions, facts, or BI grains.
[Root README](../../README.md)

# 0004 — SCD1 current-state marts

## Context
Current BI questions focus on the latest state of customers, drivers, trucks, trailers, facilities, and routes. The implemented gold layer has 13 models across dimensions and six incremental facts; snapshots record customer, driver, and truck history, but adding SCD2 joins to every fact and mart would add validity joins and rewrite cost without a current requirement.

## Decision
Dimensions overwrite current values with deterministic surrogate keys and an Unknown member (`-1`). Facts join to the current dimension row, and marts denormalise those facts into six BI grains with guarded ratios. There is no validity-date SCD2 join in the current implementation.

## Consequences
- BI queries are simpler and faster.
- Historical attribution is unavailable in the current implementation.
- Adding history later is a dimension change, not a fact rewrite.
