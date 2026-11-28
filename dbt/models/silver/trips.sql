{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'trip_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['trip_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types, validate values, derive MPG and idle metrics

WITH source AS (
    SELECT
        trip_id,
        load_id,
        driver_id,
        truck_id,
        trailer_id,
        dispatch_date,
        actual_distance_miles,
        actual_duration_hours,
        fuel_gallons_used,
        average_mpg,
        idle_time_hours,
        trip_status,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__trips') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        trip_id                                                 AS trip_id,
        load_id                                                 AS load_id,
        driver_id                                               AS driver_id,
        truck_id                                                AS truck_id,
        trailer_id                                              AS trailer_id,
        {{ clean_date('dispatch_date') }}                        AS dispatch_date,
        actual_distance_miles::INTEGER                           AS actual_distance_miles,
        actual_duration_hours::NUMERIC(10, 4)                    AS actual_duration_hours,
        fuel_gallons_used::NUMERIC(10, 4)                        AS fuel_gallons_used,
        average_mpg::NUMERIC(10, 4)                              AS average_mpg,
        idle_time_hours::NUMERIC(10, 4)                          AS idle_time_hours,
        INITCAP(REGEXP_REPLACE({{ clean_string('trip_status') }}, '[_\s]+', ' ', 'g'))
                                                                    AS trip_status,
        {{ clean_timestamp('loaded_at') }}                       AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        trip_id,
        load_id,
        driver_id,
        truck_id,
        trailer_id,
        CASE WHEN dispatch_date <= CURRENT_DATE THEN dispatch_date END AS dispatch_date,
        CASE WHEN actual_distance_miles >= 0 THEN actual_distance_miles END AS actual_distance_miles,
        CASE WHEN actual_duration_hours >= 0 THEN actual_duration_hours END AS actual_duration_hours,
        CASE WHEN fuel_gallons_used   >= 0 THEN fuel_gallons_used   END AS fuel_gallons_used,
        CASE WHEN average_mpg         >= 0 THEN average_mpg         END AS average_mpg,
        CASE WHEN idle_time_hours     >= 0 THEN idle_time_hours     END AS idle_time_hours,
        CASE
            WHEN trip_status IN ('Completed', 'In Transit', 'Planned', 'Cancelled')
                THEN trip_status
        END                                                         AS trip_status,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    trip_id,
    load_id,
    driver_id,
    truck_id,
    trailer_id,
    dispatch_date,
    actual_distance_miles,
    actual_duration_hours,
    fuel_gallons_used,
    average_mpg,
    idle_time_hours,
    -- distance divided by gallons burnt
    ROUND(actual_distance_miles / NULLIF(fuel_gallons_used, 0), 2) AS calculated_mpg,
    -- does the source MPG agree with distance over gallons (within 0.05)?
    CASE
        WHEN average_mpg IS NULL OR fuel_gallons_used IS NULL OR fuel_gallons_used = 0 THEN NULL
        ELSE ABS(average_mpg - ROUND(actual_distance_miles / fuel_gallons_used, 2)) <= 0.05
    END                                                             AS is_mpg_consistent,
    -- source data has idle longer than the trip itself, so share stays unknown there
    CASE
        WHEN actual_duration_hours > 0
         AND idle_time_hours <= actual_duration_hours
            THEN ROUND(idle_time_hours / actual_duration_hours, 4)
    END                                                             AS idle_share_of_duration,
    trip_status,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated
