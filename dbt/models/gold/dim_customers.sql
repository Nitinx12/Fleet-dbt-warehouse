{{ config(
    materialized = 'table',
    indexes      = [
        {'columns': ['customer_key'], 'unique': true},
        {'columns': ['customer_id'],  'unique': true}
    ],
    tags         = ['gold', 'dimension', 'scd1']
) }}

-- SCD Type 1: one current row per customer, attributes overwritten, no history kept.
-- The surrogate key is MD5(customer_id), so it is stable across rebuilds and facts can
-- compute it without a lookup. Includes an Unknown member for orphan / NULL references.
-- Rebuilt as a table so contract_tenure_years never goes stale.

WITH customers AS (
    SELECT
        customer_id,
        customer_name,
        customer_type,
        credit_terms_days,
        primary_freight_type,
        account_status,
        contract_start_date,
        annual_revenue_potential,
        silver_loaded_at
    FROM {{ ref('customers') }}
),
modelled AS (
    SELECT
        {{ gold_key('customer_id') }}                               AS customer_key,
        customer_id,
        COALESCE(customer_name, 'Unknown')                          AS customer_name,
        COALESCE(customer_type, 'Unknown')                          AS customer_type,
        credit_terms_days,
        CASE
            WHEN credit_terms_days IS NULL THEN 'Unknown'
            WHEN credit_terms_days <= 30   THEN '15-30 days'
            WHEN credit_terms_days <= 45   THEN '31-45 days'
            ELSE '46-60 days'
        END                                                         AS credit_terms_band,
        COALESCE(primary_freight_type, 'Unknown')                   AS primary_freight_type,
        COALESCE(account_status, 'Unknown')                         AS account_status,
        (account_status = 'Active')                                 AS is_active,
        contract_start_date,
        ROUND(((CURRENT_DATE - contract_start_date) / 365.25)::NUMERIC, 2)
                                                                    AS contract_tenure_years,
        annual_revenue_potential,
        silver_loaded_at,
        {{ gold_build_timestamp() }}                                AS gold_loaded_at
    FROM customers
)
SELECT
    customer_key,
    customer_id,
    customer_name,
    customer_type,
    credit_terms_days,
    credit_terms_band,
    primary_freight_type,
    account_status,
    is_active,
    contract_start_date,
    contract_tenure_years,
    annual_revenue_potential,
    silver_loaded_at,
    gold_loaded_at
FROM modelled

UNION ALL

SELECT
    {{ gold_unknown_key() }}, '-1', 'Unknown', 'Unknown', NULL, 'Unknown', 'Unknown', 'Unknown',
    NULL, NULL, NULL, NULL, NULL, {{ gold_build_timestamp() }}
