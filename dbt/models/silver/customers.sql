{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'customer_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['customer_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types and standardise columns

WITH source AS (
    SELECT
        customer_id,
        customer_name,
        customer_type,
        credit_terms_days,
        primary_freight_type,
        account_status,
        contract_start_date,
        annual_revenue_potential,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__customers') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _ingested_at DESC
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        customer_id                                         AS customer_id,
        {{ clean_string('customer_name') }}                 AS customer_name,
        INITCAP({{ clean_string('customer_type') }})        AS customer_type_raw,
        credit_terms_days::INTEGER                          AS credit_terms_days,
        INITCAP({{ clean_string('primary_freight_type') }}) AS primary_freight_type,
        INITCAP({{ clean_string('account_status') }})       AS account_status_raw,
        {{ clean_date('contract_start_date') }}             AS contract_start_date,
        annual_revenue_potential::NUMERIC(14, 2)            AS annual_revenue_potential,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
)
SELECT
    customer_id,
    customer_name,
    CASE
        WHEN customer_type_raw IN ('Spot', 'Contract', 'Dedicated') THEN customer_type_raw
        ELSE 'Unknown'
    END                                                     AS customer_type,
    CASE WHEN credit_terms_days BETWEEN 15 AND 60
         THEN credit_terms_days END                         AS credit_terms_days,
    primary_freight_type,
    CASE WHEN account_status_raw IN ('Active', 'Inactive')
         THEN account_status_raw END                        AS account_status,
    CASE WHEN contract_start_date <= CURRENT_DATE
         THEN contract_start_date END                       AS contract_start_date,
    CASE WHEN annual_revenue_potential >= 0
         THEN annual_revenue_potential END                  AS annual_revenue_potential,
    _ingested_at,
    CURRENT_TIMESTAMP                                       AS silver_loaded_at
FROM cleaned
