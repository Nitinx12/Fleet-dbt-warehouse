{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'mart_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['mart_key'], 'unique': true},
        {'columns': ['month_key']},
        {'columns': ['customer_key']},
        {'columns': ['customer_type']}
    ],
    tags                 = ['gold', 'mart', 'incremental']
) }}

-- Customer 360 / service-level mart.
-- Grain: one row per month x customer.
-- Sources: fct_loads (load month), fct_trips (dispatch month), fct_delivery_events (scheduled month).
-- Dimensions denormalised: dim_customers (+ month attributes derived from dim_date).
-- month_key is YYYYMM; -1 = facts whose date is the Unknown member.
-- Each fact is bucketed by its own date, so revenue (load month) and miles (dispatch month) can fall in
-- different months for the same load; revenue_per_trip_mile is therefore a monthly indicator, not a load margin.
-- Counts are 0 when the customer had no activity of that kind; sums are NULL in that case.
-- Additive: all counts and sums. Non-additive: on_time_rate, cancellation_rate, detention_rate,
--           avg_revenue_per_load, avg_delay_minutes, revenue_per_trip_mile.

{% set dim_attrs = [
    'c.customer_name', 'c.customer_type', 'c.credit_terms_band', 'c.primary_freight_type',
    'c.account_status', 'c.is_active', 'c.annual_revenue_potential'
] %}
{% set sources = ['load_src', 'trip_src', 'event_src'] %}

WITH load_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.customer_key,
        f.is_cancelled, f.load_status, f.weight_lbs, f.pieces, f.revenue, f.fuel_surcharge,
        f.accessorial_charges, f.total_charges,
        f.gold_loaded_at
    FROM {{ ref('fct_loads') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.load_date_key
),
trip_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.customer_key,
        f.actual_distance_miles, f.actual_duration_hours, f.idle_time_hours,
        f.gold_loaded_at
    FROM {{ ref('fct_trips') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.dispatch_date_key
),
event_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.customer_key,
        f.on_time_flag, f.has_detention, f.delay_minutes, f.detention_minutes, f.detention_hours,
        f.gold_loaded_at
    FROM {{ ref('fct_delivery_events') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.scheduled_date_key
),
months AS (
    SELECT DISTINCT
        year_number * 100 + month_number AS month_key,
        year_number, quarter_number, quarter_name, month_number, month_name, year_month, month_start_date
    FROM {{ ref('dim_date') }}
    WHERE date_key <> -1
),
affected AS (
    {% for s in sources %}
    {% if not loop.first %}UNION{% endif %}
    SELECT month_key, customer_key FROM {{ s }}
    {% if is_incremental() %}WHERE gold_loaded_at > {{ mart_watermark() }}{% endif %}

    {% endfor %}
    {% if is_incremental() %}
    UNION

    SELECT m.month_key, m.customer_key
    FROM {{ this }} AS m
    LEFT JOIN {{ ref('dim_customers') }} AS c ON c.customer_key = m.customer_key
    WHERE m.dim_hash <> {{ mart_hash(dim_attrs) }}
    {% endif %}
),
load_agg AS (
    SELECT
        s.month_key, s.customer_key,
        COUNT(*)                                                             AS loads_count,
        COUNT(*) FILTER (WHERE s.is_cancelled::BOOLEAN IS TRUE)              AS cancelled_loads,
        COUNT(*) FILTER (WHERE s.load_status IN ('Completed', 'Delivered'))  AS delivered_loads,
        SUM(s.weight_lbs)                                                    AS weight_lbs,
        SUM(s.pieces)                                                        AS pieces,
        SUM(s.revenue)                                                       AS revenue,
        SUM(s.fuel_surcharge)                                                AS fuel_surcharge,
        SUM(s.accessorial_charges)                                           AS accessorial_charges,
        SUM(s.total_charges)                                                 AS total_charges,
        SUM(s.revenue) FILTER (WHERE s.is_cancelled::BOOLEAN IS NOT TRUE)    AS revenue_excl_cancelled,
        MAX(s.gold_loaded_at)                                                AS loaded_at
    FROM load_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.customer_key = s.customer_key
    GROUP BY s.month_key, s.customer_key
),
trip_agg AS (
    SELECT
        s.month_key, s.customer_key,
        COUNT(*)                                                    AS trips_count,
        SUM(s.actual_distance_miles)                                AS distance_miles,
        SUM(s.actual_duration_hours)                                AS duration_hours,
        SUM(s.idle_time_hours)                                      AS idle_hours,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM trip_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.customer_key = s.customer_key
    GROUP BY s.month_key, s.customer_key
),
event_agg AS (
    SELECT
        s.month_key, s.customer_key,
        COUNT(*)                                                    AS delivery_events_count,
        COUNT(*) FILTER (WHERE s.on_time_flag::BOOLEAN IS TRUE)     AS on_time_events,
        COUNT(*) FILTER (WHERE s.on_time_flag::BOOLEAN IS FALSE)    AS late_events,
        COUNT(*) FILTER (WHERE s.on_time_flag IS NOT NULL)          AS on_time_known_events,
        SUM(s.delay_minutes)                                        AS delay_minutes_sum,
        COUNT(s.delay_minutes)                                      AS delay_minutes_count,
        COUNT(*) FILTER (WHERE s.has_detention::BOOLEAN IS TRUE)    AS detention_events,
        SUM(s.detention_minutes)                                    AS detention_minutes,
        SUM(s.detention_hours)                                      AS detention_hours,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM event_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.customer_key = s.customer_key
    GROUP BY s.month_key, s.customer_key
)
SELECT
    {{ mart_key(['a.month_key', 'a.customer_key']) }}               AS mart_key,

    a.month_key,
    a.customer_key,

    -- month attributes
    mo.month_start_date,
    mo.year_number,
    mo.quarter_number,
    mo.month_number,
    COALESCE(mo.month_name, 'Unknown')                              AS month_name,
    COALESCE(mo.year_month, 'Unknown')                              AS year_month,

    -- customer attributes
    c.customer_id,
    COALESCE(c.customer_name, 'Unknown')                            AS customer_name,
    COALESCE(c.customer_type, 'Unknown')                            AS customer_type,
    COALESCE(c.credit_terms_band, 'Unknown')                        AS credit_terms_band,
    COALESCE(c.primary_freight_type, 'Unknown')                     AS primary_freight_type,
    COALESCE(c.account_status, 'Unknown')                           AS account_status,
    c.is_active                                                     AS customer_is_active,
    c.annual_revenue_potential,

    -- loads / revenue
    COALESCE(ld.loads_count, 0)                                     AS loads_count,
    COALESCE(ld.cancelled_loads, 0)                                 AS cancelled_loads,
    COALESCE(ld.delivered_loads, 0)                                 AS delivered_loads,
    ld.weight_lbs,
    ld.pieces,
    ld.revenue,
    ld.fuel_surcharge,
    ld.accessorial_charges,
    ld.total_charges,
    ld.revenue_excl_cancelled,

    -- trips
    COALESCE(tp.trips_count, 0)                                     AS trips_count,
    tp.distance_miles,
    tp.duration_hours,
    tp.idle_hours,

    -- delivery events
    COALESCE(ev.delivery_events_count, 0)                           AS delivery_events_count,
    COALESCE(ev.on_time_events, 0)                                  AS on_time_events,
    COALESCE(ev.late_events, 0)                                     AS late_events,
    COALESCE(ev.on_time_known_events, 0)                            AS on_time_known_events,
    ev.delay_minutes_sum,
    COALESCE(ev.delay_minutes_count, 0)                             AS delay_minutes_count,
    COALESCE(ev.detention_events, 0)                                AS detention_events,
    ev.detention_minutes,
    ev.detention_hours,

    -- row-level ratios (non-additive)
    {{ mart_ratio('ev.on_time_events', 'ev.on_time_known_events') }} AS on_time_rate,
    {{ mart_ratio('ld.cancelled_loads', 'ld.loads_count') }}        AS cancellation_rate,
    {{ mart_ratio('ev.detention_events', 'ev.delivery_events_count') }} AS detention_rate,
    {{ mart_ratio('ld.revenue', 'ld.loads_count', 2) }}             AS avg_revenue_per_load,
    {{ mart_ratio('ev.delay_minutes_sum', 'ev.delay_minutes_count', 2) }}
                                                                    AS avg_delay_minutes,
    {{ mart_ratio('ld.revenue', 'tp.distance_miles') }}             AS revenue_per_trip_mile,

    -- housekeeping
    GREATEST(ld.loaded_at, tp.loaded_at, ev.loaded_at)              AS source_gold_loaded_at,
    {{ mart_hash(dim_attrs) }}                                      AS dim_hash,
    CURRENT_TIMESTAMP                                               AS mart_loaded_at
FROM affected AS a
LEFT JOIN load_agg  AS ld ON ld.month_key = a.month_key AND ld.customer_key = a.customer_key
LEFT JOIN trip_agg  AS tp ON tp.month_key = a.month_key AND tp.customer_key = a.customer_key
LEFT JOIN event_agg AS ev ON ev.month_key = a.month_key AND ev.customer_key = a.customer_key
LEFT JOIN months    AS mo ON mo.month_key = a.month_key
LEFT JOIN {{ ref('dim_customers') }} AS c ON c.customer_key = a.customer_key
