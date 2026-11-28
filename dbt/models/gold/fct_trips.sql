{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'trip_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['trip_key'], 'unique': true},
        {'columns': ['dispatch_date_key']},
        {'columns': ['driver_key']},
        {'columns': ['truck_key']},
        {'columns': ['customer_key']}
    ],
    tags                 = ['gold', 'fact', 'incremental']
) }}

-- Grain: one row per trip.
-- Dimensions: dim_drivers, dim_trucks, dim_trailers, dim_customers, dim_routes, dim_date (dispatch).
-- customer_key and route_key are inherited from the parent load, so trip KPIs can be sliced by
-- customer or lane without going through fct_loads. A trip is re-processed when either the trip
-- or its parent load changes in silver.
-- Additive: distance, duration, fuel, idle hours. Non-additive: MPG, speed, idle share
-- (average them as ratios of sums, never as an average of ratios).

WITH trips AS (
    SELECT
        t.trip_id,
        t.load_id,
        t.driver_id,
        t.truck_id,
        t.trailer_id,
        t.dispatch_date,
        t.trip_status,
        t.actual_distance_miles,
        t.actual_duration_hours,
        t.fuel_gallons_used,
        t.idle_time_hours,
        t.average_mpg,
        t.calculated_mpg,
        t.is_mpg_consistent,
        t.idle_share_of_duration,
        t.silver_loaded_at,
        l.customer_id                                               AS load_customer_id,
        l.route_id                                                  AS load_route_id,
        GREATEST(t.silver_loaded_at, l.silver_loaded_at)            AS source_loaded_at
    FROM {{ ref('trips') }} AS t
    LEFT JOIN {{ ref('loads') }} AS l
        ON l.load_id = t.load_id
    {% if is_incremental() %}
    WHERE t.silver_loaded_at > {{ gold_watermark() }}
       OR l.silver_loaded_at > {{ gold_watermark() }}
    {% endif %}
)
SELECT
    {{ gold_key('trip_id') }}                                       AS trip_key,
    trip_id,
    load_id,                                                        -- degenerate dimension
    {{ gold_key('load_id') }}                                       AS load_key,

    {{ gold_key('driver_id') }}                                     AS driver_key,
    {{ gold_key('truck_id') }}                                      AS truck_key,
    {{ gold_key('trailer_id') }}                                    AS trailer_key,
    {{ gold_key('load_customer_id') }}                              AS customer_key,
    {{ gold_key('load_route_id') }}                                 AS route_key,
    {{ gold_date_key('dispatch_date') }}                            AS dispatch_date_key,

    COALESCE(trip_status, 'Unknown')                                AS trip_status,

    actual_distance_miles,
    actual_duration_hours,
    fuel_gallons_used,
    idle_time_hours,
    average_mpg,
    calculated_mpg,
    is_mpg_consistent,
    idle_share_of_duration,
    ROUND(actual_distance_miles / NULLIF(actual_duration_hours, 0), 2)
                                                                    AS average_speed_mph,

    source_loaded_at                                                AS silver_loaded_at,
    CURRENT_TIMESTAMP                                               AS gold_loaded_at
FROM trips
