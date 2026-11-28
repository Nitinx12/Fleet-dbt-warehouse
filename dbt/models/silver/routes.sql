{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'route_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['route_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types and standardise text, validate lane values

WITH source AS (
    SELECT
        route_id,
        origin_city,
        origin_state,
        destination_city,
        destination_state,
        typical_distance_miles,
        base_rate_per_mile,
        fuel_surcharge_rate,
        typical_transit_days,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__routes') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY route_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        route_id                                                AS route_id,
        INITCAP(REGEXP_REPLACE({{ clean_string('origin_city') }}, '\s+', ' ', 'g'))
                                                                    AS origin_city,
        UPPER({{ clean_string('origin_state') }})                 AS origin_state,
        INITCAP(REGEXP_REPLACE({{ clean_string('destination_city') }}, '\s+', ' ', 'g'))
                                                                    AS destination_city,
        UPPER({{ clean_string('destination_state') }})            AS destination_state,
        typical_distance_miles::INTEGER                           AS typical_distance_miles,
        base_rate_per_mile::NUMERIC(10, 4)                        AS base_rate_per_mile,
        fuel_surcharge_rate::NUMERIC(10, 4)                       AS fuel_surcharge_rate,
        typical_transit_days::INTEGER                             AS typical_transit_days,
        {{ clean_timestamp('loaded_at') }}                         AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        route_id,
        origin_city,
        CASE WHEN origin_state ~ '^[A-Z]{2}$' THEN origin_state END AS origin_state,
        destination_city,
        CASE
            WHEN destination_state ~ '^[A-Z]{2}$' THEN destination_state
        END                                                         AS destination_state,
        CASE WHEN typical_distance_miles >= 0 THEN typical_distance_miles END AS typical_distance_miles,
        CASE WHEN base_rate_per_mile   >= 0 THEN base_rate_per_mile   END AS base_rate_per_mile,
        CASE WHEN fuel_surcharge_rate  >= 0 THEN fuel_surcharge_rate  END AS fuel_surcharge_rate,
        CASE WHEN typical_transit_days >= 0 THEN typical_transit_days END AS typical_transit_days,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    route_id,
    origin_city,
    origin_state,
    destination_city,
    destination_state,
    typical_distance_miles,
    base_rate_per_mile,
    fuel_surcharge_rate,
    typical_transit_days,
    -- rate per mile applied to the typical lane distance
    ROUND(typical_distance_miles * base_rate_per_mile, 2)         AS base_lane_revenue,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated
