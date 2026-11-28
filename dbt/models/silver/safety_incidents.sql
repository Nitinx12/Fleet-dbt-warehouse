{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'incident_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['incident_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast types, standardise text and flags, validate values, derive cost metrics
-- Bronze already uses the correct column names (vehicle_damage_cost, cargo_damage_cost)

WITH source AS (
    SELECT
        incident_id,
        trip_id,
        truck_id,
        driver_id,
        incident_date,
        incident_type,
        location_city,
        location_state,
        at_fault_flag,
        injury_flag,
        vehicle_damage_cost,
        cargo_damage_cost,
        claim_amount,
        preventable_flag,
        description,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mongo__safety_incidents') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY incident_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        incident_id                                           AS incident_id,
        trip_id                                               AS trip_id,
        truck_id                                              AS truck_id,
        driver_id                                             AS driver_id,
        {{ clean_date('incident_date') }}                           AS incident_date,
        INITCAP(REGEXP_REPLACE({{ clean_string('incident_type') }}, '\s+', ' ', 'g'))
                                                                    AS incident_type,
        INITCAP(REGEXP_REPLACE({{ clean_string('location_city') }}, '\s+', ' ', 'g'))
                                                                    AS location_city,
        UPPER({{ clean_string('location_state') }})                 AS location_state,
        {{ clean_boolean('at_fault_flag') }}                        AS at_fault_flag,
        {{ clean_boolean('injury_flag') }}                          AS injury_flag,
        vehicle_damage_cost::NUMERIC(12, 2)                   AS vehicle_damage_cost,
        cargo_damage_cost::NUMERIC(12, 2)                     AS cargo_damage_cost,
        claim_amount::NUMERIC(12, 2)                                AS claim_amount,
        {{ clean_boolean('preventable_flag') }}                     AS preventable_flag,
        REGEXP_REPLACE({{ clean_string('description') }}, '\s+', ' ', 'g')
                                                                    AS description,
        {{ clean_timestamp('loaded_at') }}                          AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        incident_id,
        trip_id,
        truck_id,
        driver_id,
        CASE WHEN incident_date <= CURRENT_DATE THEN incident_date END AS incident_date,
        incident_type,
        location_city,
        CASE WHEN location_state ~ '^[A-Z]{2}$' THEN location_state END AS location_state,
        at_fault_flag,
        injury_flag,
        CASE WHEN vehicle_damage_cost >= 0 THEN vehicle_damage_cost END AS vehicle_damage_cost,
        CASE WHEN cargo_damage_cost   >= 0 THEN cargo_damage_cost   END AS cargo_damage_cost,
        CASE WHEN claim_amount        >= 0 THEN claim_amount        END AS claim_amount,
        preventable_flag,
        description,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    incident_id,
    trip_id,
    truck_id,
    driver_id,
    incident_date,
    incident_type,
    location_city,
    location_state,
    at_fault_flag,
    injury_flag,
    vehicle_damage_cost,
    cargo_damage_cost,
    -- vehicle + cargo, only when both are known
    (vehicle_damage_cost + cargo_damage_cost)                       AS total_damage_cost,
    claim_amount,
    -- NULL claim = unknown; 0 = no claim filed
    CASE WHEN claim_amount IS NULL THEN NULL ELSE claim_amount > 0 END AS has_claim,
    preventable_flag,
    description,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated


