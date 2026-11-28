{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'fuel_purchase_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['fuel_purchase_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types, standardise text, validate values, derive cost checks

WITH source AS (
    SELECT
        fuel_purchase_id,
        trip_id,
        truck_id,
        driver_id,
        purchase_date,
        location_city,
        location_state,
        gallons,
        price_per_gallon,
        total_cost,
        fuel_card_number,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__fuel_purchases') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY fuel_purchase_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        fuel_purchase_id                                        AS fuel_purchase_id,
        trip_id                                                 AS trip_id,
        truck_id                                                AS truck_id,
        driver_id                                               AS driver_id,
        {{ clean_timestamp('purchase_date') }}                   AS purchase_date,
        INITCAP(REGEXP_REPLACE({{ clean_string('location_city') }}, '\s+', ' ', 'g'))
                                                                    AS location_city,
        UPPER({{ clean_string('location_state') }})              AS location_state,
        gallons::NUMERIC(10, 4)                                  AS gallons,
        price_per_gallon::NUMERIC(10, 4)                         AS price_per_gallon,
        total_cost::NUMERIC(12, 2)                               AS total_cost,
        {{ clean_string('fuel_card_number') }}                   AS fuel_card_number,
        {{ clean_timestamp('loaded_at') }}                       AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        fuel_purchase_id,
        trip_id,
        truck_id,
        driver_id,
        -- a purchase cannot be in the future
        CASE
            WHEN purchase_date <= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '1 day'
                THEN purchase_date
        END                                                         AS purchase_date,
        location_city,
        CASE WHEN location_state ~ '^[A-Z]{2}$' THEN location_state END AS location_state,
        CASE WHEN gallons          >= 0 THEN gallons          END    AS gallons,
        CASE WHEN price_per_gallon >= 0 THEN price_per_gallon END    AS price_per_gallon,
        CASE WHEN total_cost       >= 0 THEN total_cost       END    AS total_cost,
        fuel_card_number,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    fuel_purchase_id,
    trip_id,
    truck_id,
    driver_id,
    purchase_date,
    location_city,
    location_state,
    gallons,
    price_per_gallon,
    total_cost,
    -- gallons times price, rounded to the cent
    ROUND(gallons * price_per_gallon, 2)                         AS calculated_total_cost,
    -- does the source total agree with gallons times price (within 1 cent)?
    CASE
        WHEN total_cost IS NULL OR gallons IS NULL OR price_per_gallon IS NULL THEN NULL
        ELSE ABS(total_cost - ROUND(gallons * price_per_gallon, 2)) <= 0.01
    END                                                             AS is_total_cost_consistent,
    fuel_card_number,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated
