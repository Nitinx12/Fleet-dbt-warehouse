{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'event_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['event_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast types, standardise text, validate values, derive delay and detention metrics

WITH source AS (
    SELECT
        event_id,
        load_id,
        trip_id,
        facility_id,
        event_type,
        scheduled_datetime,
        actual_datetime,
        detention_minutes,
        on_time_flag,
        location_city,
        location_state,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mongo__delivery_events') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        event_id                                                AS event_id,
        load_id                                               AS load_id,
        trip_id                                              AS trip_id,
        facility_id                                          AS facility_id,
        UPPER(REGEXP_REPLACE({{ clean_string('event_type') }}, '\s+', '_', 'g'))
                                                                    AS event_type,
        {{ clean_timestamp('scheduled_datetime') }}                 AS scheduled_datetime,
        {{ clean_timestamp('actual_datetime') }}                    AS actual_datetime,
        detention_minutes::NUMERIC(10, 2)                           AS detention_minutes,
        {{ clean_boolean('on_time_flag') }}                     AS on_time_flag,

        INITCAP(REGEXP_REPLACE({{ clean_string('location_city') }}, '\s+', ' ', 'g'))
                                                                    AS location_city,
        UPPER({{ clean_string('location_state') }})                 AS location_state,
        {{ clean_timestamp('loaded_at') }}                          AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        event_id,
        load_id,
        trip_id,
        facility_id,
        event_type,
        scheduled_datetime,
        -- an actual time more than a day in the future is bad data (1 day allows for clock skew)
        CASE
            WHEN actual_datetime <= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '1 day'
                THEN actual_datetime
        END                                                         AS actual_datetime,
        CASE WHEN detention_minutes >= 0 THEN detention_minutes END AS detention_minutes,
        on_time_flag,
        location_city,
        CASE WHEN location_state ~ '^[A-Z]{2}$' THEN location_state END AS location_state,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    event_id,
    load_id,
    trip_id,
    facility_id,
    event_type,
    scheduled_datetime,
    actual_datetime,
    -- positive = late, negative = early
    ROUND(
        (EXTRACT(EPOCH FROM (actual_datetime - scheduled_datetime)) / 60.0)::NUMERIC,
        2
    )                                                               AS delay_minutes,
    detention_minutes,
    ROUND(detention_minutes / 60.0, 2)                              AS detention_hours,
    on_time_flag,
    location_city,
    location_state,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated


