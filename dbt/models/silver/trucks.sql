{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'truck_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['truck_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types, standardise text, validate values, derive age and availability

WITH source AS (
    SELECT
        truck_id,
        unit_number,
        make,
        model_year,
        vin,
        acquisition_date,
        acquisition_mileage,
        fuel_type,
        tank_capacity_gallons,
        status,
        home_terminal,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__trucks') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY truck_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        truck_id                                                AS truck_id,
        unit_number::INTEGER                                    AS unit_number,
        INITCAP({{ clean_string('make') }})                      AS make,
        model_year::INTEGER                                     AS model_year,
        UPPER(REGEXP_REPLACE({{ clean_string('vin') }}, '[^A-Z0-9]', '', 'g'))
                                                                    AS vin,
        {{ clean_date('acquisition_date') }}                     AS acquisition_date,
        acquisition_mileage::INTEGER                            AS acquisition_mileage,
        INITCAP({{ clean_string('fuel_type') }})                 AS fuel_type,
        tank_capacity_gallons::INTEGER                          AS tank_capacity_gallons,
        INITCAP(REGEXP_REPLACE({{ clean_string('status') }}, '[_\s]+', ' ', 'g'))
                                                                    AS status,
        INITCAP(REGEXP_REPLACE({{ clean_string('home_terminal') }}, '\s+', ' ', 'g'))
                                                                    AS home_terminal,
        {{ clean_timestamp('loaded_at') }}                       AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        truck_id,
        unit_number,
        make,
        -- a model year cannot be in the future or before trucks existed
        CASE
            WHEN model_year BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE)::INT
                THEN model_year
        END                                                         AS model_year,
        NULLIF(vin, '')                                            AS vin,
        CASE WHEN acquisition_date <= CURRENT_DATE THEN acquisition_date END AS acquisition_date,
        CASE WHEN acquisition_mileage >= 0 THEN acquisition_mileage END AS acquisition_mileage,
        fuel_type,
        CASE WHEN tank_capacity_gallons >= 0 THEN tank_capacity_gallons END AS tank_capacity_gallons,
        CASE
            WHEN status IN ('Active', 'Maintenance', 'Inactive')
                THEN status
            ELSE 'Unknown'
        END                                                         AS status,
        home_terminal,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    truck_id,
    unit_number,
    make,
    model_year,
    vin,
    acquisition_date,
    acquisition_mileage,
    fuel_type,
    tank_capacity_gallons,
    status,
    -- only an Active truck is available for dispatch
    status = 'Active'                                              AS is_available,
    EXTRACT(YEAR FROM CURRENT_DATE)::INT - model_year              AS age_years,
    home_terminal,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated
