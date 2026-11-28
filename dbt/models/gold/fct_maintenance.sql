{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'maintenance_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['maintenance_key'], 'unique': true},
        {'columns': ['maintenance_date_key']},
        {'columns': ['truck_key']}
    ],
    tags                 = ['gold', 'fact', 'incremental']
) }}

-- Grain: one row per maintenance work order.
-- Dimensions: dim_trucks, dim_date (maintenance date).
-- Additive: labour_hours, labour_cost, parts_cost, total_cost, downtime_hours.
-- Non-additive: labour_cost_per_hour, odometer_reading.

WITH maintenance AS (
    SELECT
        maintenance_id,
        truck_id,
        maintenance_date,
        maintenance_type,
        facility_location,
        service_description,
        odometer_reading,
        labour_hours,
        labour_cost,
        parts_cost,
        total_cost,
        calculated_total_cost,
        is_total_cost_consistent,
        labour_cost_per_hour,
        downtime_hours,
        silver_loaded_at
    FROM {{ ref('maintenance_records') }}
    {% if is_incremental() %}
    WHERE silver_loaded_at > {{ gold_watermark() }}
    {% endif %}
)
SELECT
    {{ gold_key('maintenance_id') }}                                AS maintenance_key,
    maintenance_id,

    {{ gold_key('truck_id') }}                                      AS truck_key,
    {{ gold_date_key('maintenance_date') }}                         AS maintenance_date_key,

    COALESCE(maintenance_type, 'Unknown')                           AS maintenance_type,
    facility_location,
    service_description,

    odometer_reading,
    labour_hours,
    labour_cost,
    parts_cost,
    total_cost,
    calculated_total_cost,
    is_total_cost_consistent,
    labour_cost_per_hour,
    downtime_hours,

    silver_loaded_at,
    CURRENT_TIMESTAMP                                               AS gold_loaded_at
FROM maintenance
