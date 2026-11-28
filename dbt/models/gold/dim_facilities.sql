{{ config(
    materialized = 'table',
    indexes      = [
        {'columns': ['facility_key'], 'unique': true},
        {'columns': ['facility_id'],  'unique': true}
    ],
    tags         = ['gold', 'dimension']
) }}

-- One current row per facility (Type 1) plus an Unknown member.

WITH facilities AS (
    SELECT
        facility_id,
        facility_name,
        facility_type,
        city,
        state,
        latitude,
        longitude,
        dock_doors,
        operating_hours,
        silver_loaded_at
    FROM {{ ref('facilities') }}
),
modelled AS (
    SELECT
        {{ gold_key('facility_id') }}                               AS facility_key,
        facility_id,
        COALESCE(facility_name, 'Unknown')                          AS facility_name,
        COALESCE(facility_type, 'Unknown')                          AS facility_type,
        COALESCE(city, 'Unknown')                                   AS city,
        state,
        CASE
            WHEN city IS NOT NULL AND state IS NOT NULL THEN city || ', ' || state
            ELSE 'Unknown'
        END                                                         AS city_state,
        latitude,
        longitude,
        dock_doors,
        operating_hours,
        silver_loaded_at,
        CURRENT_TIMESTAMP                                           AS gold_loaded_at
    FROM facilities
)
SELECT
    facility_key,
    facility_id,
    facility_name,
    facility_type,
    city,
    state,
    city_state,
    latitude,
    longitude,
    dock_doors,
    operating_hours,
    silver_loaded_at,
    gold_loaded_at
FROM modelled

UNION ALL

SELECT
    {{ gold_unknown_key() }}, '-1', 'Unknown', 'Unknown', 'Unknown', NULL, 'Unknown',
    NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP
