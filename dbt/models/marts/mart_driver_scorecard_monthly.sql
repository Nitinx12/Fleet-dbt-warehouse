{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'mart_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['mart_key'], 'unique': true},
        {'columns': ['month_key']},
        {'columns': ['driver_key']},
        {'columns': ['home_terminal']}
    ],
    tags                 = ['gold', 'mart', 'incremental', 'pii']
) }}

-- Driver scorecard mart.
-- Grain: one row per month x driver.
-- Sources: fct_trips (dispatch month), fct_delivery_events (scheduled month),
--          fct_fuel_purchases (purchase month), fct_safety_incidents (incident month).
-- Dimensions denormalised: dim_drivers (+ month attributes derived from dim_date).
-- month_key is YYYYMM; -1 = facts whose date is the Unknown member.
-- PII: driver_name is the display name carried from dim_drivers. Drop it (and re-run with --full-refresh)
--      or apply row-level security in the BI tool if the audience should not see names.
-- Counts are 0 when the driver had no activity of that kind in the month; sums are NULL in that case.
-- Additive: all counts, sums and *_miles / *_gallons inputs.
-- Non-additive: fleet_mpg, on_time_rate, avg_delay_minutes, avg_fuel_price_per_gallon,
--          incidents_per_million_miles (re-compute from the additive columns when rolling up).

{% set dim_attrs = [
    'dr.full_name', 'dr.home_terminal', 'dr.employment_status', 'dr.is_active', 'dr.cdl_class',
    'dr.license_state', 'dr.age_band', 'dr.experience_band', 'dr.tenure_band'
] %}
{% set sources = ['trip_src', 'event_src', 'fuel_src', 'safety_src'] %}

WITH trip_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.driver_key,
        f.actual_distance_miles, f.actual_duration_hours, f.fuel_gallons_used, f.idle_time_hours,
        f.gold_loaded_at
    FROM {{ ref('fct_trips') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.dispatch_date_key
),
event_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.driver_key,
        f.on_time_flag, f.has_detention, f.delay_minutes, f.detention_minutes, f.detention_hours,
        f.gold_loaded_at
    FROM {{ ref('fct_delivery_events') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.scheduled_date_key
),
fuel_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.driver_key,
        f.gallons, f.total_cost, f.is_total_cost_consistent,
        f.gold_loaded_at
    FROM {{ ref('fct_fuel_purchases') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.purchase_date_key
),
safety_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.driver_key,
        f.at_fault_flag, f.injury_flag, f.preventable_flag, f.has_claim,
        f.total_damage_cost, f.claim_amount,
        f.gold_loaded_at
    FROM {{ ref('fct_safety_incidents') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.incident_date_key
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
    SELECT month_key, driver_key FROM {{ s }}
    {% if is_incremental() %}WHERE gold_loaded_at > {{ mart_watermark() }}{% endif %}

    {% endfor %}
    {% if is_incremental() %}
    UNION

    SELECT m.month_key, m.driver_key
    FROM {{ this }} AS m
    LEFT JOIN {{ ref('dim_drivers') }} AS dr ON dr.driver_key = m.driver_key
    WHERE m.dim_hash <> {{ mart_hash(dim_attrs) }}
    {% endif %}
),
trip_agg AS (
    SELECT
        s.month_key, s.driver_key,
        COUNT(*)                                                    AS trips_count,
        SUM(s.actual_distance_miles)                                AS distance_miles,
        SUM(s.actual_duration_hours)                                AS duration_hours,
        SUM(s.fuel_gallons_used)                                    AS trip_fuel_gallons,
        SUM(s.idle_time_hours)                                      AS idle_hours,
        SUM(s.actual_distance_miles)
            FILTER (WHERE s.fuel_gallons_used > 0 AND s.actual_distance_miles IS NOT NULL) AS mpg_miles,
        SUM(s.fuel_gallons_used)
            FILTER (WHERE s.fuel_gallons_used > 0 AND s.actual_distance_miles IS NOT NULL) AS mpg_gallons,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM trip_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.driver_key = s.driver_key
    GROUP BY s.month_key, s.driver_key
),
event_agg AS (
    SELECT
        s.month_key, s.driver_key,
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
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.driver_key = s.driver_key
    GROUP BY s.month_key, s.driver_key
),
fuel_agg AS (
    SELECT
        s.month_key, s.driver_key,
        COUNT(*)                                                    AS fuel_purchases_count,
        SUM(s.gallons)                                              AS fuel_gallons_purchased,
        SUM(s.total_cost)                                           AS fuel_cost,
        COUNT(*) FILTER (WHERE s.is_total_cost_consistent::BOOLEAN IS FALSE)
                                                                    AS fuel_cost_inconsistent_purchases,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM fuel_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.driver_key = s.driver_key
    GROUP BY s.month_key, s.driver_key
),
safety_agg AS (
    SELECT
        s.month_key, s.driver_key,
        COUNT(*)                                                    AS incidents_count,
        COUNT(*) FILTER (WHERE s.at_fault_flag::BOOLEAN IS TRUE)    AS at_fault_incidents,
        COUNT(*) FILTER (WHERE s.injury_flag::BOOLEAN IS TRUE)      AS injury_incidents,
        COUNT(*) FILTER (WHERE s.preventable_flag::BOOLEAN IS TRUE) AS preventable_incidents,
        COUNT(*) FILTER (WHERE s.has_claim::BOOLEAN IS TRUE)        AS claims_count,
        SUM(s.total_damage_cost)                                    AS damage_cost,
        SUM(s.claim_amount)                                         AS claim_amount,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM safety_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.driver_key = s.driver_key
    GROUP BY s.month_key, s.driver_key
)
SELECT
    {{ mart_key(['a.month_key', 'a.driver_key']) }}                 AS mart_key,

    a.month_key,
    a.driver_key,

    -- month attributes
    mo.month_start_date,
    mo.year_number,
    mo.quarter_number,
    mo.month_number,
    COALESCE(mo.month_name, 'Unknown')                              AS month_name,
    COALESCE(mo.year_month, 'Unknown')                              AS year_month,

    -- driver attributes
    dr.driver_id,
    COALESCE(dr.full_name, 'Unknown')                               AS driver_name,
    COALESCE(dr.home_terminal, 'Unknown')                           AS home_terminal,
    COALESCE(dr.employment_status, 'Unknown')                       AS employment_status,
    dr.is_active                                                    AS driver_is_active,
    COALESCE(dr.cdl_class, 'Unknown')                               AS cdl_class,
    dr.license_state,
    COALESCE(dr.age_band, 'Unknown')                                AS age_band,
    COALESCE(dr.experience_band, 'Unknown')                         AS experience_band,
    COALESCE(dr.tenure_band, 'Unknown')                             AS tenure_band,

    -- trips
    COALESCE(tp.trips_count, 0)                                     AS trips_count,
    tp.distance_miles,
    tp.duration_hours,
    tp.trip_fuel_gallons,
    tp.idle_hours,
    tp.mpg_miles,
    tp.mpg_gallons,

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

    -- fuel
    COALESCE(fu.fuel_purchases_count, 0)                            AS fuel_purchases_count,
    fu.fuel_gallons_purchased,
    fu.fuel_cost,
    COALESCE(fu.fuel_cost_inconsistent_purchases, 0)                AS fuel_cost_inconsistent_purchases,

    -- safety
    COALESCE(sa.incidents_count, 0)                                 AS incidents_count,
    COALESCE(sa.at_fault_incidents, 0)                              AS at_fault_incidents,
    COALESCE(sa.injury_incidents, 0)                                AS injury_incidents,
    COALESCE(sa.preventable_incidents, 0)                           AS preventable_incidents,
    COALESCE(sa.claims_count, 0)                                    AS claims_count,
    sa.damage_cost,
    sa.claim_amount,

    -- row-level ratios (non-additive)
    {{ mart_ratio('tp.mpg_miles', 'tp.mpg_gallons', 2) }}           AS fleet_mpg,
    {{ mart_ratio('ev.on_time_events', 'ev.on_time_known_events') }} AS on_time_rate,
    {{ mart_ratio('ev.delay_minutes_sum', 'ev.delay_minutes_count', 2) }}
                                                                    AS avg_delay_minutes,
    {{ mart_ratio('fu.fuel_cost', 'fu.fuel_gallons_purchased') }}   AS avg_fuel_price_per_gallon,
    {{ mart_ratio('COALESCE(sa.incidents_count, 0) * 1000000.0', 'tp.distance_miles', 4) }}
                                                                    AS incidents_per_million_miles,

    -- housekeeping
    GREATEST(tp.loaded_at, ev.loaded_at, fu.loaded_at, sa.loaded_at) AS source_gold_loaded_at,
    {{ mart_hash(dim_attrs) }}                                      AS dim_hash,
    CURRENT_TIMESTAMP                                               AS mart_loaded_at
FROM affected AS a
LEFT JOIN trip_agg   AS tp ON tp.month_key = a.month_key AND tp.driver_key = a.driver_key
LEFT JOIN event_agg  AS ev ON ev.month_key = a.month_key AND ev.driver_key = a.driver_key
LEFT JOIN fuel_agg   AS fu ON fu.month_key = a.month_key AND fu.driver_key = a.driver_key
LEFT JOIN safety_agg AS sa ON sa.month_key = a.month_key AND sa.driver_key = a.driver_key
LEFT JOIN months     AS mo ON mo.month_key = a.month_key
LEFT JOIN {{ ref('dim_drivers') }} AS dr ON dr.driver_key = a.driver_key
