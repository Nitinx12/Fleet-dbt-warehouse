{{ config(
    materialized = 'table',
    indexes      = [
        {'columns': ['trailer_key'], 'unique': true},
        {'columns': ['trailer_id'],  'unique': true}
    ],
    tags         = ['gold', 'dimension']
) }}

-- One current row per trailer (Type 1) plus an Unknown member.

WITH trailers AS (
    SELECT
        trailer_id,
        trailer_number,
        trailer_type,
        length_feet,
        model_year,
        vin,
        acquisition_date,
        status,
        current_location,
        silver_loaded_at
    FROM {{ ref('trailers') }}
),
derived AS (
    SELECT
        trailer_id,
        trailer_number,
        trailer_type,
        length_feet,
        model_year,
        vin,
        acquisition_date,
        status,
        current_location,
        silver_loaded_at,
        EXTRACT(YEAR FROM CURRENT_DATE)::INT - model_year           AS age_years_current
    FROM trailers
),
modelled AS (
    SELECT
        {{ gold_key('trailer_id') }}                                AS trailer_key,
        trailer_id,
        trailer_number,
        COALESCE(trailer_type, 'Unknown')                           AS trailer_type,
        length_feet,
        model_year,
        vin,
        acquisition_date,
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
        COALESCE(current_location, 'Unknown')                       AS current_location,
        silver_loaded_at,
        CURRENT_TIMESTAMP                                           AS gold_loaded_at
    FROM derived
)
SELECT
    trailer_key,
    trailer_id,
    trailer_number,
    trailer_type,
    length_feet,
    model_year,
    vin,
    acquisition_date,
    status,
    is_available,
    age_years,
    age_band,
    current_location,
    silver_loaded_at,
    gold_loaded_at
FROM modelled

UNION ALL

SELECT
    {{ gold_unknown_key() }}, '-1', NULL, 'Unknown', NULL, NULL, NULL, NULL, 'Unknown', NULL,
    NULL, 'Unknown', 'Unknown', NULL, CURRENT_TIMESTAMP
