{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'mart_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['mart_key'], 'unique': true},
        {'columns': ['month_key']},
        {'columns': ['truck_key']},
        {'columns': ['truck_home_terminal']}
    ],
    tags                 = ['gold', 'mart', 'incremental']
) }}

-- Truck cost-of-ownership / operating cost mart.
-- Grain: one row per month x truck.
-- Sources: fct_fuel_purchases (purchase month), fct_maintenance (maintenance month),
--          fct_safety_incidents (incident month), fct_trips (dispatch month).
-- Dimensions denormalised: dim_trucks (+ month attributes derived from dim_date).
-- month_key is YYYYMM; -1 = facts whose date is the Unknown member.
-- Counts are 0 when the truck had no activity of that kind; component sums are NULL in that case.
-- total_operating_cost = fuel_cost + maintenance_cost + damage_cost, with unknown components treated as 0;
-- use the component columns when you need to distinguish "no cost" from "cost unknown".
-- Additive: counts, costs, hours, miles, gallons. Non-additive: *_per_mile, fleet_mpg,
--          avg_fuel_price_per_gallon, max_odometer_reading.

{% set dim_attrs = [
    't.unit_number', 't.make', 't.model_year', 't.fuel_type', 't.tank_capacity_gallons', 't.status',
    't.is_available', 't.age_band', 't.home_terminal'
] %}
{% set sources = ['fuel_src', 'maint_src', 'safety_src', 'trip_src'] %}

WITH fuel_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.truck_key,
        f.gallons, f.total_cost,
        f.gold_loaded_at
    FROM {{ ref('fct_fuel_purchases') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.purchase_date_key
),
maint_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.truck_key,
        f.odometer_reading, f.labour_hours, f.labour_cost, f.parts_cost, f.total_cost, f.downtime_hours,
        f.gold_loaded_at
    FROM {{ ref('fct_maintenance') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.maintenance_date_key
),
safety_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.truck_key,
        f.total_damage_cost, f.claim_amount,
        f.gold_loaded_at
    FROM {{ ref('fct_safety_incidents') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.incident_date_key
),
trip_src AS NOT MATERIALIZED (
    SELECT
        {{ mart_month_key('d') }}  AS month_key,
        f.truck_key,
        f.actual_distance_miles, f.actual_duration_hours, f.fuel_gallons_used,
        f.gold_loaded_at
    FROM {{ ref('fct_trips') }} AS f
    LEFT JOIN {{ ref('dim_date') }} AS d ON d.date_key = f.dispatch_date_key
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
    SELECT month_key, truck_key FROM {{ s }}
    {% if is_incremental() %}WHERE gold_loaded_at > {{ mart_watermark() }}{% endif %}

    {% endfor %}
    {% if is_incremental() %}
    UNION

    SELECT m.month_key, m.truck_key
    FROM {{ this }} AS m
    LEFT JOIN {{ ref('dim_trucks') }} AS t ON t.truck_key = m.truck_key
    WHERE m.dim_hash <> {{ mart_hash(dim_attrs) }}
    {% endif %}
),
fuel_agg AS (
    SELECT
        s.month_key, s.truck_key,
        COUNT(*)                                                    AS fuel_purchases_count,
        SUM(s.gallons)                                              AS fuel_gallons,
        SUM(s.total_cost)                                           AS fuel_cost,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM fuel_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.truck_key = s.truck_key
    GROUP BY s.month_key, s.truck_key
),
maint_agg AS (
    SELECT
        s.month_key, s.truck_key,
        COUNT(*)                                                    AS maintenance_orders,
        SUM(s.labour_hours)                                         AS labour_hours,
        SUM(s.labour_cost)                                          AS labour_cost,
        SUM(s.parts_cost)                                           AS parts_cost,
        SUM(s.total_cost)                                           AS maintenance_cost,
        SUM(s.downtime_hours)                                       AS downtime_hours,
        MAX(s.odometer_reading)                                     AS max_odometer_reading,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM maint_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.truck_key = s.truck_key
    GROUP BY s.month_key, s.truck_key
),
safety_agg AS (
    SELECT
        s.month_key, s.truck_key,
        COUNT(*)                                                    AS incidents_count,
        SUM(s.total_damage_cost)                                    AS damage_cost,
        SUM(s.claim_amount)                                         AS claim_amount,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM safety_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.truck_key = s.truck_key
    GROUP BY s.month_key, s.truck_key
),
trip_agg AS (
    SELECT
        s.month_key, s.truck_key,
        COUNT(*)                                                    AS trips_count,
        SUM(s.actual_distance_miles)                                AS distance_miles,
        SUM(s.actual_duration_hours)                                AS duration_hours,
        SUM(s.actual_distance_miles)
            FILTER (WHERE s.fuel_gallons_used > 0 AND s.actual_distance_miles IS NOT NULL) AS mpg_miles,
        SUM(s.fuel_gallons_used)
            FILTER (WHERE s.fuel_gallons_used > 0 AND s.actual_distance_miles IS NOT NULL) AS mpg_gallons,
        MAX(s.gold_loaded_at)                                       AS loaded_at
    FROM trip_src AS s
    INNER JOIN affected AS a ON a.month_key = s.month_key AND a.truck_key = s.truck_key
    GROUP BY s.month_key, s.truck_key
)
SELECT
    {{ mart_key(['a.month_key', 'a.truck_key']) }}                  AS mart_key,

    a.month_key,
    a.truck_key,

    -- month attributes
    mo.month_start_date,
    mo.year_number,
    mo.quarter_number,
    mo.month_number,
    COALESCE(mo.month_name, 'Unknown')                              AS month_name,
    COALESCE(mo.year_month, 'Unknown')                              AS year_month,

    -- truck attributes
    t.truck_id,
    t.unit_number,
    COALESCE(t.make, 'Unknown')                                     AS truck_make,
    t.model_year                                                    AS truck_model_year,
    COALESCE(t.fuel_type, 'Unknown')                                AS truck_fuel_type,
    t.tank_capacity_gallons                                         AS truck_tank_capacity_gallons,
    COALESCE(t.status, 'Unknown')                                   AS truck_status,
    t.is_available                                                  AS truck_is_available,
    COALESCE(t.age_band, 'Unknown')                                 AS truck_age_band,
    COALESCE(t.home_terminal, 'Unknown')                            AS truck_home_terminal,

    -- trips
    COALESCE(tp.trips_count, 0)                                     AS trips_count,
    tp.distance_miles,
    tp.duration_hours,
    tp.mpg_miles,
    tp.mpg_gallons,

    -- fuel
    COALESCE(fu.fuel_purchases_count, 0)                            AS fuel_purchases_count,
    fu.fuel_gallons,
    fu.fuel_cost,

    -- maintenance
    COALESCE(mt.maintenance_orders, 0)                              AS maintenance_orders,
    mt.labour_hours,
    mt.labour_cost,
    mt.parts_cost,
    mt.maintenance_cost,
    mt.downtime_hours,
    mt.max_odometer_reading,

    -- safety
    COALESCE(sa.incidents_count, 0)                                 AS incidents_count,
    sa.damage_cost,
    sa.claim_amount,

    -- total cost
    COALESCE(fu.fuel_cost, 0) + COALESCE(mt.maintenance_cost, 0) + COALESCE(sa.damage_cost, 0)
                                                                    AS total_operating_cost,

    -- row-level ratios (non-additive)
    {{ mart_ratio('COALESCE(fu.fuel_cost, 0) + COALESCE(mt.maintenance_cost, 0) + COALESCE(sa.damage_cost, 0)', 'tp.distance_miles') }}
                                                                    AS operating_cost_per_mile,
    {{ mart_ratio('fu.fuel_cost', 'tp.distance_miles') }}           AS fuel_cost_per_mile,
    {{ mart_ratio('mt.maintenance_cost', 'tp.distance_miles') }}    AS maintenance_cost_per_mile,
    {{ mart_ratio('tp.mpg_miles', 'tp.mpg_gallons', 2) }}           AS fleet_mpg,
    {{ mart_ratio('fu.fuel_cost', 'fu.fuel_gallons') }}             AS avg_fuel_price_per_gallon,

    -- housekeeping
    GREATEST(tp.loaded_at, fu.loaded_at, mt.loaded_at, sa.loaded_at) AS source_gold_loaded_at,
    {{ mart_hash(dim_attrs) }}                                      AS dim_hash,
    CURRENT_TIMESTAMP                                               AS mart_loaded_at
FROM affected AS a
LEFT JOIN trip_agg   AS tp ON tp.month_key = a.month_key AND tp.truck_key = a.truck_key
LEFT JOIN fuel_agg   AS fu ON fu.month_key = a.month_key AND fu.truck_key = a.truck_key
LEFT JOIN maint_agg  AS mt ON mt.month_key = a.month_key AND mt.truck_key = a.truck_key
LEFT JOIN safety_agg AS sa ON sa.month_key = a.month_key AND sa.truck_key = a.truck_key
LEFT JOIN months     AS mo ON mo.month_key = a.month_key
LEFT JOIN {{ ref('dim_trucks') }} AS t ON t.truck_key = a.truck_key
