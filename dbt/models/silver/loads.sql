{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'load_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['load_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types, standardise text, validate values, derive charge totals

WITH source AS (
    SELECT
        load_id,
        customer_id,
        route_id,
        load_date,
        load_type,
        weight_lbs,
        pieces,
        revenue,
        fuel_surcharge,
        accessorial_charges,
        load_status,
        booking_type,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__loads') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY load_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        load_id                                                 AS load_id,
        customer_id                                             AS customer_id,
        route_id                                                AS route_id,
        {{ clean_date('load_date') }}                            AS load_date,
        INITCAP(REGEXP_REPLACE({{ clean_string('load_type') }}, '\s+', ' ', 'g'))
                                                                    AS load_type,
        weight_lbs::INTEGER                                     AS weight_lbs,
        pieces::INTEGER                                         AS pieces,
        revenue::NUMERIC(12, 2)                                  AS revenue,
        fuel_surcharge::NUMERIC(10, 4)                           AS fuel_surcharge,
        accessorial_charges::NUMERIC(12, 2)                      AS accessorial_charges,
        INITCAP(REGEXP_REPLACE({{ clean_string('load_status') }}, '[_\s]+', ' ', 'g'))
                                                                    AS load_status,
        INITCAP({{ clean_string('booking_type') }})              AS booking_type,
        {{ clean_timestamp('loaded_at') }}                       AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        load_id,
        customer_id,
        route_id,
        CASE WHEN load_date <= CURRENT_DATE THEN load_date END   AS load_date,
        CASE
            WHEN load_type IN ('Dry Van', 'Refrigerated', 'Flatbed', 'Tanker')
                THEN load_type
        END                                                         AS load_type,
        CASE WHEN weight_lbs   >= 0 THEN weight_lbs   END          AS weight_lbs,
        CASE WHEN pieces       >= 0 THEN pieces       END          AS pieces,
        CASE WHEN revenue      >= 0 THEN revenue      END          AS revenue,
        CASE WHEN fuel_surcharge >= 0 THEN fuel_surcharge END      AS fuel_surcharge,
        CASE WHEN accessorial_charges >= 0 THEN accessorial_charges END AS accessorial_charges,
        CASE
            WHEN load_status IN ('Completed', 'In Transit', 'Delivered', 'Cancelled', 'Booked')
                THEN load_status
        END                                                         AS load_status,
        CASE
            WHEN booking_type IN ('Dedicated', 'Contract', 'Spot')
                THEN booking_type
        END                                                         AS booking_type,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    load_id,
    customer_id,
    route_id,
    load_date,
    load_type,
    weight_lbs,
    pieces,
    revenue,
    fuel_surcharge,
    accessorial_charges,
    -- base revenue plus surcharges, only when every component is known
    (revenue + fuel_surcharge + accessorial_charges)              AS total_charges,
    load_status,
    booking_type,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated
