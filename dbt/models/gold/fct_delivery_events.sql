{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'event_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['event_key'], 'unique': true},
        {'columns': ['scheduled_date_key']},
        {'columns': ['facility_key']},
        {'columns': ['driver_key']},
        {'columns': ['customer_key']}
    ],
    tags                 = ['gold', 'fact', 'incremental']
) }}

-- Grain: one row per delivery event (pickup / delivery stop).
-- Dimensions: dim_facilities, dim_drivers, dim_trucks, dim_customers, dim_date (scheduled and actual).
-- driver_key / truck_key come from the parent trip and customer_key from the parent load.
-- scheduled_date_key and actual_date_key are role-playing keys into dim_date.
-- Additive: detention_minutes, detention_hours. Non-additive: delay_minutes (average it).

WITH events AS (
    SELECT
        e.event_id,
        e.trip_id,
        e.load_id,
        e.facility_id,
        e.event_type,
        e.scheduled_datetime,
        e.actual_datetime,
        e.location_city,
        e.location_state,
        e.on_time_flag,
        e.delay_minutes,
        e.detention_minutes,
        e.detention_hours,
        e.silver_loaded_at,
        COALESCE(e.load_id, t.load_id)                              AS resolved_load_id,
        t.driver_id                                                 AS trip_driver_id,
        t.truck_id                                                  AS trip_truck_id,
        l.customer_id                                               AS load_customer_id,
        GREATEST(e.silver_loaded_at, t.silver_loaded_at, l.silver_loaded_at)
                                                                    AS source_loaded_at
    FROM {{ ref('delivery_events') }} AS e
    LEFT JOIN {{ ref('trips') }} AS t
        ON t.trip_id = e.trip_id
    LEFT JOIN {{ ref('loads') }} AS l
        ON l.load_id = COALESCE(e.load_id, t.load_id)
    {% if is_incremental() %}
    WHERE e.silver_loaded_at > {{ gold_watermark() }}
       OR t.silver_loaded_at > {{ gold_watermark() }}
       OR l.silver_loaded_at > {{ gold_watermark() }}
    {% endif %}
)
SELECT
    {{ gold_key('event_id') }}                                      AS event_key,
    event_id,
    trip_id,                                                        -- degenerate dimensions
    resolved_load_id                                                AS load_id,
    {{ gold_key('trip_id') }}                                       AS trip_key,
    {{ gold_key('resolved_load_id') }}                              AS load_key,

    {{ gold_key('facility_id') }}                                   AS facility_key,
    {{ gold_key('trip_driver_id') }}                                AS driver_key,
    {{ gold_key('trip_truck_id') }}                                 AS truck_key,
    {{ gold_key('load_customer_id') }}                              AS customer_key,
    {{ gold_date_key('scheduled_datetime') }}                       AS scheduled_date_key,
    {{ gold_date_key('actual_datetime') }}                          AS actual_date_key,

    COALESCE(event_type, 'Unknown')                                 AS event_type,
    scheduled_datetime,
    actual_datetime,
    location_city,                                                  -- location as recorded on the event
    location_state,

    on_time_flag,
    (detention_minutes > 0)                                         AS has_detention,
    delay_minutes,
    detention_minutes,
    detention_hours,

    source_loaded_at                                                AS silver_loaded_at,
    CURRENT_TIMESTAMP                                               AS gold_loaded_at
FROM events
