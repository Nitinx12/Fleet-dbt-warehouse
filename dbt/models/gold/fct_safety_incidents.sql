{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'incident_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['incident_key'], 'unique': true},
        {'columns': ['incident_date_key']},
        {'columns': ['driver_key']},
        {'columns': ['truck_key']}
    ],
    tags                 = ['gold', 'fact', 'incremental']
) }}

-- Grain: one row per safety incident.
-- Dimensions: dim_drivers, dim_trucks, dim_date (incident date); trip is a degenerate key.
-- PII: the free-text description is intentionally excluded from gold.
-- Additive: damage costs and claim_amount. Boolean flags can be summed as counts.

WITH incidents AS (
    SELECT
        incident_id,
        trip_id,
        driver_id,
        truck_id,
        incident_date,
        incident_type,
        location_city,
        location_state,
        at_fault_flag,
        injury_flag,
        preventable_flag,
        has_claim,
        vehicle_damage_cost,
        cargo_damage_cost,
        total_damage_cost,
        claim_amount,
        silver_loaded_at
    FROM {{ ref('safety_incidents') }}
    {% if is_incremental() %}
    WHERE silver_loaded_at > {{ gold_watermark() }}
    {% endif %}
)
SELECT
    {{ gold_key('incident_id') }}                                   AS incident_key,
    incident_id,
    trip_id,
    {{ gold_key('trip_id') }}                                       AS trip_key,

    {{ gold_key('driver_id') }}                                     AS driver_key,
    {{ gold_key('truck_id') }}                                      AS truck_key,
    {{ gold_date_key('incident_date') }}                            AS incident_date_key,

    COALESCE(incident_type, 'Unknown')                              AS incident_type,
    location_city,
    location_state,

    at_fault_flag,
    injury_flag,
    preventable_flag,
    has_claim,

    vehicle_damage_cost,
    cargo_damage_cost,
    total_damage_cost,
    claim_amount,

    silver_loaded_at,
    CURRENT_TIMESTAMP                                               AS gold_loaded_at
FROM incidents
