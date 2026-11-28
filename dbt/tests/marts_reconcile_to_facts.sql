-- Fails when any mart stops adding back to the gold fact(s) it is built from.
SELECT
    check_name,
    fact_value,
    mart_value,
    difference
FROM {{ ref('assert_marts_reconcile_to_facts') }}
