{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'mart_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['mart_key'], 'unique': true},
        {'columns': ['dispatch_date_key']},
        {'columns': ['truck_key']},
        {'columns': ['trailer_key']},
        {'columns': ['year_month']}
    ],
    tags                 = ['gold', 'mart', 'incremental']
) }}

-- Fleet / equipment utilisation mart.
-- Grain: one row per dispatch date x truck x trailer.
-- Source: fct_trips. Dimensions denormalised: dim_date, dim_trucks, dim_trailers.
-- Additive: trips_count, distance_miles, duration_hours, fuel_gallons_used, idle_hours and the
--           *_miles / *_gallons / *_hours inputs.
-- Non-additive: fleet_mpg, avg_speed_mph, idle_share, avg_trip_*. When rolling up in BI use
--           SUM(mpg_miles) / SUM(mpg_gallons), SUM(speed_miles) / SUM(speed_hours),
--           SUM(idle_hours) / SUM(duration_hours); never AVG() the ratio columns.
-- mpg_* and speed_* only include trips where both inputs are known and > 0, so the ratios are not
-- diluted by trips with missing fuel or duration.

{% set dim_attrs = [
    't.unit_number', 't.make', 't.model_year', 't.fuel_type', 't.status', 't.is_available',
    't.age_band', 't.home_terminal',
    'tr.trailer_number', 'tr.trailer_type', 'tr.length_feet', 'tr.status', 'tr.age_band'
] %}

WITH affected AS (
    SELECT DISTINCT dispatch_date_key, truck_key, trailer_key
    FROM {{ ref('fct_trips') }}
    {% if is_incremental() %}
    WHERE gold_loaded_at > {{ mart_watermark() }}

    UNION

    SELECT m.dispatch_date_key, m.truck_key, m.trailer_key
    FROM {{ this }} AS m
    LEFT JOIN {{ ref('dim_trucks') }}   AS t  ON t.truck_key     = m.truck_key
    LEFT JOIN {{ ref('dim_trailers') }} AS tr ON tr.trailer_key  = m.trailer_key
    WHERE m.dim_hash <> {{ mart_hash(dim_attrs) }}
    {% endif %}
),
aggregated AS (
    SELECT
        f.dispatch_date_key,
        f.truck_key,
        f.trailer_key,

        COUNT(*)                                                    AS trips_count,
        SUM(f.actual_distance_miles)                                AS distance_miles,
        SUM(f.actual_duration_hours)                                AS duration_hours,
        SUM(f.fuel_gallons_used)                                    AS fuel_gallons_used,
        SUM(f.idle_time_hours)                                      AS idle_hours,

        SUM(f.actual_distance_miles)
            FILTER (WHERE f.fuel_gallons_used > 0 AND f.actual_distance_miles IS NOT NULL)
                                                                    AS mpg_miles,
        SUM(f.fuel_gallons_used)
            FILTER (WHERE f.fuel_gallons_used > 0 AND f.actual_distance_miles IS NOT NULL)
                                                                    AS mpg_gallons,
        SUM(f.actual_distance_miles)
            FILTER (WHERE f.actual_duration_hours > 0 AND f.actual_distance_miles IS NOT NULL)
                                                                    AS speed_miles,
        SUM(f.actual_duration_hours)
            FILTER (WHERE f.actual_duration_hours > 0 AND f.actual_distance_miles IS NOT NULL)
                                                                    AS speed_hours,
        COUNT(*) FILTER (WHERE f.is_mpg_consistent::BOOLEAN IS FALSE)
                                                                    AS mpg_inconsistent_trips,

        MAX(f.gold_loaded_at)                                       AS source_gold_loaded_at
    FROM {{ ref('fct_trips') }} AS f
    INNER JOIN affected AS a
        ON  a.dispatch_date_key = f.dispatch_date_key
        AND a.truck_key         = f.truck_key
        AND a.trailer_key       = f.trailer_key
    GROUP BY f.dispatch_date_key, f.truck_key, f.trailer_key
)
SELECT
    {{ mart_key(['g.dispatch_date_key', 'g.truck_key', 'g.trailer_key']) }}  AS mart_key,

    g.dispatch_date_key,
    g.truck_key,
    g.trailer_key,

    -- date attributes
    d.date_day                                                      AS dispatch_date,
    d.year_number,
    d.quarter_number,
    d.month_number,
    d.month_name,
    d.year_month,
    d.iso_year_number,
    d.iso_week_number,
    d.day_name,
    d.is_weekend,
    d.week_start_date,
    d.month_start_date,

    -- truck attributes
    t.truck_id,
    t.unit_number,
    COALESCE(t.make, 'Unknown')                                     AS truck_make,
    t.model_year                                                    AS truck_model_year,
    COALESCE(t.fuel_type, 'Unknown')                                AS truck_fuel_type,
    COALESCE(t.status, 'Unknown')                                   AS truck_status,
    t.is_available                                                  AS truck_is_available,
    COALESCE(t.age_band, 'Unknown')                                 AS truck_age_band,
    COALESCE(t.home_terminal, 'Unknown')                            AS truck_home_terminal,

    -- trailer attributes
    tr.trailer_id,
    tr.trailer_number,
    COALESCE(tr.trailer_type, 'Unknown')                            AS trailer_type,
    tr.length_feet                                                  AS trailer_length_feet,
    COALESCE(tr.status, 'Unknown')                                  AS trailer_status,
    COALESCE(tr.age_band, 'Unknown')                                AS trailer_age_band,

    -- measures
    g.trips_count,
    g.distance_miles,
    g.duration_hours,
    g.fuel_gallons_used,
    g.idle_hours,
    g.mpg_miles,
    g.mpg_gallons,
    g.speed_miles,
    g.speed_hours,
    g.mpg_inconsistent_trips,

    -- row-level ratios (non-additive)
    {{ mart_ratio('g.mpg_miles', 'g.mpg_gallons', 2) }}             AS fleet_mpg,
    {{ mart_ratio('g.speed_miles', 'g.speed_hours', 2) }}           AS avg_speed_mph,
    {{ mart_ratio('g.idle_hours', 'g.duration_hours') }}            AS idle_share,
    {{ mart_ratio('g.distance_miles', 'g.trips_count', 2) }}        AS avg_trip_miles,
    {{ mart_ratio('g.duration_hours', 'g.trips_count', 2) }}        AS avg_trip_hours,

    -- housekeeping
    g.source_gold_loaded_at,
    {{ mart_hash(dim_attrs) }}                                      AS dim_hash,
    CURRENT_TIMESTAMP                                               AS mart_loaded_at
FROM aggregated AS g
LEFT JOIN {{ ref('dim_date') }}     AS d  ON d.date_key      = g.dispatch_date_key
LEFT JOIN {{ ref('dim_trucks') }}   AS t  ON t.truck_key     = g.truck_key
LEFT JOIN {{ ref('dim_trailers') }} AS tr ON tr.trailer_key  = g.trailer_key
