{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'maintenance_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['maintenance_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Read the full bronze table
-- Remove duplicates, keeping the latest ingested record
-- Cast types, standardise text, validate values, derive cost metrics

WITH source AS (
    SELECT
        maintenance_id,
        truck_id,
        maintenance_date,
        maintenance_type,
        odometer_reading,
        labor_hours,
        labor_cost,
        parts_cost,
        total_cost,
        facility_location,
        downtime_hours,
        service_description,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mongo__maintenance_records') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY maintenance_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        maintenance_id                                          AS maintenance_id,
        truck_id                                                AS truck_id,
        {{ clean_date('maintenance_date') }}                        AS maintenance_date,
        INITCAP(REGEXP_REPLACE({{ clean_string('maintenance_type') }}, '\s+', ' ', 'g'))
                                                                    AS maintenance_type,
        odometer_reading::NUMERIC(12, 1)                            AS odometer_reading,
        labor_hours::NUMERIC(8, 2)                               AS labour_hours,
        labor_cost::NUMERIC(12, 2)                               AS labour_cost,
        parts_cost::NUMERIC(12, 2)                                  AS parts_cost,
        total_cost::NUMERIC(12, 2)                                  AS total_cost,
        REGEXP_REPLACE({{ clean_string('facility_location') }}, '\s+', ' ', 'g')
                                                                    AS facility_location,
        downtime_hours::NUMERIC(8, 2)                               AS downtime_hours,
        REGEXP_REPLACE({{ clean_string('service_description') }}, '\s+', ' ', 'g')
                                                                    AS service_description,
        {{ clean_timestamp('loaded_at') }}                          AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        maintenance_id,
        truck_id,
        CASE WHEN maintenance_date <= CURRENT_DATE THEN maintenance_date END AS maintenance_date,
        maintenance_type,
        CASE WHEN odometer_reading >= 0 THEN odometer_reading END            AS odometer_reading,
        CASE WHEN labour_hours     >= 0 THEN labour_hours     END            AS labour_hours,
        CASE WHEN labour_cost      >= 0 THEN labour_cost      END            AS labour_cost,
        CASE WHEN parts_cost       >= 0 THEN parts_cost       END            AS parts_cost,
        CASE WHEN total_cost       >= 0 THEN total_cost       END            AS total_cost,
        facility_location,
        CASE WHEN downtime_hours   >= 0 THEN downtime_hours   END            AS downtime_hours,
        service_description,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    maintenance_id,
    truck_id,
    maintenance_date,
    maintenance_type,
    odometer_reading,
    labour_hours,
    labour_cost,
    parts_cost,
    total_cost,
    -- labour + parts, only when both are known
    (labour_cost + parts_cost)                                      AS calculated_total_cost,
    -- does the source total agree with labour + parts (within 1 cent)?
    CASE
        WHEN total_cost IS NULL OR labour_cost IS NULL OR parts_cost IS NULL THEN NULL
        ELSE ABS(total_cost - (labour_cost + parts_cost)) <= 0.01
    END                                                             AS is_total_cost_consistent,
    ROUND(labour_cost / NULLIF(labour_hours, 0), 2)                 AS labour_cost_per_hour,
    facility_location,
    downtime_hours,
    service_description,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated

