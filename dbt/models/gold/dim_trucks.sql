{{ config(
    materialized = 'table',
    indexes      = [
        {'columns': ['truck_key'], 'unique': true},
        {'columns': ['truck_id'],  'unique': true}
    ],
    tags         = ['gold', 'dimension']
) }}

-- One current row per truck (Type 1) plus an Unknown member.
-- age_years is recomputed here so it stays correct for rows silver has not re-ingested.

WITH trucks AS (
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
        silver_loaded_at
    FROM {{ ref('trucks') }}
),
derived AS (
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
        silver_loaded_at,
        EXTRACT(YEAR FROM CURRENT_DATE)::INT - model_year           AS age_years_current
    FROM trucks
),
modelled AS (
    SELECT
        {{ gold_key('truck_id') }}                                  AS truck_key,
        truck_id,
        unit_number,
        COALESCE(make, 'Unknown')                                   AS make,
        model_year,
        vin,
        acquisition_date,
        acquisition_mileage,
        COALESCE(fuel_type, 'Unknown')                              AS fuel_type,
        tank_capacity_gallons,
        COALESCE(status, 'Unknown')                                 AS status,
        (status = 'Active')                                         AS is_available,
        age_years_current                                           AS age_years,
        CASE
            WHEN age_years_current IS NULL THEN 'Unknown'
            WHEN age_years_current <= 2    THEN '0-2 years'
            WHEN age_years_current <= 5    THEN '3-5 years'
            WHEN age_years_current <= 10   THEN '6-10 years'
            ELSE '11+ years'
        END                                                         AS age_band,
        COALESCE(home_terminal, 'Unknown')                          AS home_terminal,
        silver_loaded_at,
        CURRENT_TIMESTAMP                                           AS gold_loaded_at
    FROM derived
)
SELECT
    truck_key,
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
    is_available,
    age_years,
    age_band,
    home_terminal,
    silver_loaded_at,
    gold_loaded_at
FROM modelled

UNION ALL

SELECT
    {{ gold_unknown_key() }}, '-1', NULL, 'Unknown', NULL, NULL, NULL, NULL, 'Unknown', NULL,
    'Unknown', NULL, NULL, 'Unknown', 'Unknown', NULL, CURRENT_TIMESTAMP
