{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'mart_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['mart_key'], 'unique': true},
        {'columns': ['load_date_key']},
        {'columns': ['customer_key']},
        {'columns': ['route_key']},
        {'columns': ['year_month']}
    ],
    tags                 = ['gold', 'mart', 'incremental']
) }}

-- Sales / revenue mart.
-- Grain: one row per load date x customer x route (lane).
-- Source: fct_loads. Dimensions denormalised: dim_date, dim_customers, dim_routes.
-- Additive: loads_count, *_loads, weight_lbs, pieces, revenue, fuel_surcharge, accessorial_charges,
--           total_charges, *_excl_cancelled, total_typical_miles.
-- Non-additive (ratios, re-compute as ratio of sums when rolling up): avg_revenue_per_load,
--           cancellation_rate, revenue_per_typical_mile.
--
-- Incremental logic: a group is re-aggregated from the full fact when any of its fact rows changed
-- (fct_loads.gold_loaded_at > watermark) or when its customer / route attributes changed (dim_hash).

{% set dim_attrs = [
    'c.customer_name', 'c.customer_type', 'c.credit_terms_band', 'c.primary_freight_type',
    'c.account_status', 'c.is_active',
    'r.lane_name', 'r.origin_city', 'r.origin_state', 'r.destination_city', 'r.destination_state',
    'r.is_intrastate', 'r.typical_distance_miles', 'r.haul_type'
] %}

WITH affected AS (
    SELECT DISTINCT load_date_key, customer_key, route_key
    FROM {{ ref('fct_loads') }}
    {% if is_incremental() %}
    WHERE gold_loaded_at > {{ mart_watermark() }}

    UNION

    SELECT m.load_date_key, m.customer_key, m.route_key
    FROM {{ this }} AS m
    LEFT JOIN {{ ref('dim_customers') }} AS c ON c.customer_key = m.customer_key
    LEFT JOIN {{ ref('dim_routes') }}    AS r ON r.route_key    = m.route_key
    WHERE m.dim_hash <> {{ mart_hash(dim_attrs) }}
    {% endif %}
),
aggregated AS (
    SELECT
        f.load_date_key,
        f.customer_key,
        f.route_key,

        COUNT(*)                                                             AS loads_count,
        COUNT(*) FILTER (WHERE f.is_cancelled::BOOLEAN IS TRUE)              AS cancelled_loads,
        COUNT(*) FILTER (WHERE f.load_status IN ('Completed', 'Delivered'))  AS delivered_loads,
        COUNT(*) FILTER (WHERE f.load_status = 'In Transit')                 AS in_transit_loads,
        COUNT(*) FILTER (WHERE f.load_status = 'Booked')                     AS booked_loads,
        COUNT(*) FILTER (WHERE f.total_charges IS NULL)                      AS loads_with_unknown_charges,

        SUM(f.weight_lbs)                                                    AS weight_lbs,
        SUM(f.pieces)                                                        AS pieces,
        SUM(f.revenue)                                                       AS revenue,
        SUM(f.fuel_surcharge)                                                AS fuel_surcharge,
        SUM(f.accessorial_charges)                                           AS accessorial_charges,
        SUM(f.total_charges)                                                 AS total_charges,
        SUM(f.revenue)       FILTER (WHERE f.is_cancelled::BOOLEAN IS NOT TRUE) AS revenue_excl_cancelled,
        SUM(f.total_charges) FILTER (WHERE f.is_cancelled::BOOLEAN IS NOT TRUE) AS total_charges_excl_cancelled,

        MAX(f.gold_loaded_at)                                                AS source_gold_loaded_at
    FROM {{ ref('fct_loads') }} AS f
    INNER JOIN affected AS a
        ON  a.load_date_key = f.load_date_key
        AND a.customer_key  = f.customer_key
        AND a.route_key     = f.route_key
    GROUP BY f.load_date_key, f.customer_key, f.route_key
)
SELECT
    {{ mart_key(['g.load_date_key', 'g.customer_key', 'g.route_key']) }}      AS mart_key,

    g.load_date_key,
    g.customer_key,
    g.route_key,

    -- date attributes
    d.date_day                                                      AS load_date,
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

    -- customer attributes
    c.customer_id,
    COALESCE(c.customer_name, 'Unknown')                            AS customer_name,
    COALESCE(c.customer_type, 'Unknown')                            AS customer_type,
    COALESCE(c.credit_terms_band, 'Unknown')                        AS credit_terms_band,
    COALESCE(c.primary_freight_type, 'Unknown')                     AS primary_freight_type,
    COALESCE(c.account_status, 'Unknown')                           AS account_status,
    c.is_active                                                     AS customer_is_active,

    -- lane attributes
    r.route_id,
    COALESCE(r.lane_name, 'Unknown')                                AS lane_name,
    COALESCE(r.origin_city, 'Unknown')                              AS origin_city,
    r.origin_state,
    COALESCE(r.destination_city, 'Unknown')                         AS destination_city,
    r.destination_state,
    r.is_intrastate,
    COALESCE(r.haul_type, 'Unknown')                                AS haul_type,
    r.typical_distance_miles,

    -- measures
    g.loads_count,
    g.cancelled_loads,
    g.delivered_loads,
    g.in_transit_loads,
    g.booked_loads,
    g.loads_with_unknown_charges,
    g.weight_lbs,
    g.pieces,
    g.revenue,
    g.fuel_surcharge,
    g.accessorial_charges,
    g.total_charges,
    g.revenue_excl_cancelled,
    g.total_charges_excl_cancelled,
    g.loads_count * r.typical_distance_miles                        AS total_typical_miles,

    -- row-level ratios (non-additive)
    {{ mart_ratio('g.revenue', 'g.loads_count', 2) }}               AS avg_revenue_per_load,
    {{ mart_ratio('g.cancelled_loads', 'g.loads_count') }}          AS cancellation_rate,
    {{ mart_ratio('g.revenue', 'g.loads_count * r.typical_distance_miles') }}
                                                                    AS revenue_per_typical_mile,

    -- housekeeping
    g.source_gold_loaded_at,
    {{ mart_hash(dim_attrs) }}                                      AS dim_hash,
    CURRENT_TIMESTAMP                                               AS mart_loaded_at
FROM aggregated AS g
LEFT JOIN {{ ref('dim_date') }}      AS d ON d.date_key     = g.load_date_key
LEFT JOIN {{ ref('dim_customers') }} AS c ON c.customer_key = g.customer_key
LEFT JOIN {{ ref('dim_routes') }}    AS r ON r.route_key    = g.route_key
