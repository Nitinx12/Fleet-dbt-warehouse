{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'mart_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['mart_key'], 'unique': true},
        {'columns': ['scheduled_date_key']},
        {'columns': ['facility_key']},
        {'columns': ['event_type']},
        {'columns': ['year_month']}
    ],
    tags                 = ['gold', 'mart', 'incremental']
) }}

-- Delivery / on-time performance mart.
-- Grain: one row per scheduled date x facility x event type (pickup / delivery).
-- Source: fct_delivery_events. Dimensions denormalised: dim_date (scheduled), dim_facilities.
-- Additive: events_count, completed_events, open_events, on_time_events, late_events,
--           on_time_known_events, delay_minutes_sum, delay_minutes_count, detention_*.
-- Non-additive: on_time_rate, avg_delay_minutes, max_delay_minutes, detention_rate, avg_detention_minutes.
-- Roll up on_time_rate as SUM(on_time_events) / SUM(on_time_known_events) and avg delay as
-- SUM(delay_minutes_sum) / SUM(delay_minutes_count).
-- open_events = events with no actual time yet (not delivered / picked up).

{% set dim_attrs = [
    'fa.facility_name', 'fa.facility_type', 'fa.city', 'fa.state', 'fa.city_state',
    'fa.latitude', 'fa.longitude', 'fa.dock_doors', 'fa.operating_hours'
] %}

WITH affected AS (
    SELECT DISTINCT scheduled_date_key, facility_key, event_type
    FROM {{ ref('fct_delivery_events') }}
    {% if is_incremental() %}
    WHERE gold_loaded_at > {{ mart_watermark() }}

    UNION

    SELECT m.scheduled_date_key, m.facility_key, m.event_type
    FROM {{ this }} AS m
    LEFT JOIN {{ ref('dim_facilities') }} AS fa ON fa.facility_key = m.facility_key
    WHERE m.dim_hash <> {{ mart_hash(dim_attrs) }}
    {% endif %}
),
aggregated AS (
    SELECT
        f.scheduled_date_key,
        f.facility_key,
        f.event_type,

        COUNT(*)                                                    AS events_count,
        COUNT(*) FILTER (WHERE f.actual_datetime IS NOT NULL)       AS completed_events,
        COUNT(*) FILTER (WHERE f.actual_datetime IS NULL)           AS open_events,
        COUNT(*) FILTER (WHERE f.on_time_flag::BOOLEAN IS TRUE)     AS on_time_events,
        COUNT(*) FILTER (WHERE f.on_time_flag::BOOLEAN IS FALSE)    AS late_events,
        COUNT(*) FILTER (WHERE f.on_time_flag IS NOT NULL)          AS on_time_known_events,

        SUM(f.delay_minutes)                                        AS delay_minutes_sum,
        COUNT(f.delay_minutes)                                      AS delay_minutes_count,
        MAX(f.delay_minutes)                                        AS max_delay_minutes,

        COUNT(*) FILTER (WHERE f.has_detention::BOOLEAN IS TRUE)    AS detention_events,
        SUM(f.detention_minutes)                                    AS detention_minutes,
        SUM(f.detention_hours)                                      AS detention_hours,

        MAX(f.gold_loaded_at)                                       AS source_gold_loaded_at
    FROM {{ ref('fct_delivery_events') }} AS f
    INNER JOIN affected AS a
        ON  a.scheduled_date_key = f.scheduled_date_key
        AND a.facility_key       = f.facility_key
        AND a.event_type         = f.event_type
    GROUP BY f.scheduled_date_key, f.facility_key, f.event_type
)
SELECT
    {{ mart_key(['g.scheduled_date_key', 'g.facility_key', 'g.event_type']) }}  AS mart_key,

    g.scheduled_date_key,
    g.facility_key,
    g.event_type,

    -- date attributes (scheduled date)
    d.date_day                                                      AS scheduled_date,
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

    -- facility attributes
    fa.facility_id,
    COALESCE(fa.facility_name, 'Unknown')                           AS facility_name,
    COALESCE(fa.facility_type, 'Unknown')                           AS facility_type,
    COALESCE(fa.city, 'Unknown')                                    AS facility_city,
    fa.state                                                        AS facility_state,
    COALESCE(fa.city_state, 'Unknown')                              AS facility_city_state,
    fa.latitude                                                     AS facility_latitude,
    fa.longitude                                                    AS facility_longitude,
    fa.dock_doors                                                   AS facility_dock_doors,
    fa.operating_hours                                              AS facility_operating_hours,

    -- measures
    g.events_count,
    g.completed_events,
    g.open_events,
    g.on_time_events,
    g.late_events,
    g.on_time_known_events,
    g.delay_minutes_sum,
    g.delay_minutes_count,
    g.max_delay_minutes,
    g.detention_events,
    g.detention_minutes,
    g.detention_hours,

    -- row-level ratios (non-additive)
    {{ mart_ratio('g.on_time_events', 'g.on_time_known_events') }}  AS on_time_rate,
    {{ mart_ratio('g.delay_minutes_sum', 'g.delay_minutes_count', 2) }}
                                                                    AS avg_delay_minutes,
    {{ mart_ratio('g.detention_events', 'g.events_count') }}        AS detention_rate,
    {{ mart_ratio('g.detention_minutes', 'g.detention_events', 2) }} AS avg_detention_minutes,

    -- housekeeping
    g.source_gold_loaded_at,
    {{ mart_hash(dim_attrs) }}                                      AS dim_hash,
    CURRENT_TIMESTAMP                                               AS mart_loaded_at
FROM aggregated AS g
LEFT JOIN {{ ref('dim_date') }}       AS d  ON d.date_key        = g.scheduled_date_key
LEFT JOIN {{ ref('dim_facilities') }} AS fa ON fa.facility_key   = g.facility_key
