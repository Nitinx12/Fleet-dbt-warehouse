{{ config(
    materialized = 'table',
    indexes      = [
        {'columns': ['route_key'], 'unique': true},
        {'columns': ['route_id'],  'unique': true}
    ],
    tags         = ['gold', 'dimension']
) }}

-- One current row per lane (Type 1) plus an Unknown member.
-- haul_type thresholds (250 / 1000 miles) are a modelling assumption; adjust to your business.

WITH routes AS (
    SELECT
        route_id,
        origin_city,
        origin_state,
        destination_city,
        destination_state,
        typical_distance_miles,
        base_rate_per_mile,
        fuel_surcharge_rate,
        typical_transit_days,
        base_lane_revenue,
        silver_loaded_at
    FROM {{ ref('routes') }}
),
modelled AS (
    SELECT
        {{ gold_key('route_id') }}                                  AS route_key,
        route_id,
        COALESCE(origin_city, 'Unknown')                            AS origin_city,
        origin_state,
        COALESCE(destination_city, 'Unknown')                       AS destination_city,
        destination_state,
        CASE
            WHEN origin_city IS NULL OR origin_state IS NULL
              OR destination_city IS NULL OR destination_state IS NULL THEN 'Unknown'
            ELSE origin_city || ', ' || origin_state || ' to ' || destination_city || ', ' || destination_state
        END                                                         AS lane_name,
        (origin_state = destination_state)                          AS is_intrastate,
        typical_distance_miles,
        CASE
            WHEN typical_distance_miles IS NULL THEN 'Unknown'
            WHEN typical_distance_miles < 250   THEN 'Short Haul'
            WHEN typical_distance_miles < 1000  THEN 'Medium Haul'
            ELSE 'Long Haul'
        END                                                         AS haul_type,
        base_rate_per_mile,
        fuel_surcharge_rate,
        typical_transit_days,
        base_lane_revenue,
        silver_loaded_at,
        {{ gold_build_timestamp() }}                                AS gold_loaded_at
    FROM routes
)
SELECT
    route_key,
    route_id,
    origin_city,
    origin_state,
    destination_city,
    destination_state,
    lane_name,
    is_intrastate,
    typical_distance_miles,
    haul_type,
    base_rate_per_mile,
    fuel_surcharge_rate,
    typical_transit_days,
    base_lane_revenue,
    silver_loaded_at,
    gold_loaded_at
FROM modelled

UNION ALL

SELECT
    {{ gold_unknown_key() }}, '-1', 'Unknown', NULL, 'Unknown', NULL, 'Unknown', NULL,
    NULL, 'Unknown', NULL, NULL, NULL, NULL, NULL, {{ gold_build_timestamp() }}
