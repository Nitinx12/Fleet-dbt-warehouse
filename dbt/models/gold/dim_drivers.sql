{{ config(
    materialized = 'table',
    indexes      = [
        {'columns': ['driver_key'], 'unique': true},
        {'columns': ['driver_id'],  'unique': true}
    ],
    tags         = ['gold', 'dimension', 'scd1', 'pii']
) }}

-- SCD Type 1: one current row per driver, attributes overwritten, no history kept.
--
-- PII handling:
--   * license_number  -> SHA-256 hash (license_number_hash), salted with GOLD_PII_SALT, joinable but unreadable
--   * date_of_birth   -> dropped; age_years / age_band are kept
--   * first/last name -> dropped; full_name is kept as the display name
--
-- age_years and tenure_years are recomputed here from the silver dates. Silver is incremental,
-- so its stored values only refresh when a row is re-ingested and would drift for unchanged rows.

WITH drivers AS (
    SELECT
        driver_id,
        full_name,
        hire_date,
        termination_date,
        license_number,
        license_state,
        date_of_birth,
        home_terminal,
        employment_status,
        is_active,
        is_employment_consistent,
        cdl_class,
        years_experience,
        tenure_years,
        silver_loaded_at
    FROM {{ ref('drivers') }}
),
derived AS (
    SELECT
        driver_id,
        full_name,
        hire_date,
        termination_date,
        license_number,
        license_state,
        date_of_birth,
        home_terminal,
        employment_status,
        is_active,
        is_employment_consistent,
        cdl_class,
        years_experience,
        tenure_years,
        silver_loaded_at,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth))::INT    AS age_years_current,
        -- a terminated driver with no valid termination date has unknown tenure
        CASE
            WHEN employment_status = 'Terminated' AND termination_date IS NULL THEN NULL
            ELSE ROUND(
                ((COALESCE(termination_date, CURRENT_DATE) - hire_date) / 365.25)::NUMERIC, 2
            )
        END                                                         AS tenure_years_current
    FROM drivers
),
modelled AS (
    SELECT
        {{ gold_key('driver_id') }}                                 AS driver_key,
        driver_id,
        COALESCE(full_name, 'Unknown')                              AS full_name,
        hire_date,
        termination_date,
        {{ gold_hash_pii('license_number') }}                       AS license_number_hash,
        license_state,
        age_years_current                                           AS age_years,
        CASE
            WHEN age_years_current IS NULL THEN 'Unknown'
            WHEN age_years_current < 25    THEN '18-24'
            WHEN age_years_current < 35    THEN '25-34'
            WHEN age_years_current < 45    THEN '35-44'
            WHEN age_years_current < 55    THEN '45-54'
            WHEN age_years_current < 65    THEN '55-64'
            ELSE '65+'
        END                                                         AS age_band,
        COALESCE(home_terminal, 'Unknown')                          AS home_terminal,
        COALESCE(employment_status, 'Unknown')                      AS employment_status,
        is_active,
        is_employment_consistent,
        COALESCE(cdl_class, 'Unknown')                              AS cdl_class,
        years_experience,
        CASE
            WHEN years_experience IS NULL THEN 'Unknown'
            WHEN years_experience < 1     THEN '<1 year'
            WHEN years_experience < 5     THEN '1-4 years'
            WHEN years_experience < 10    THEN '5-9 years'
            WHEN years_experience < 20    THEN '10-19 years'
            ELSE '20+ years'
        END                                                         AS experience_band,
        tenure_years_current                                        AS tenure_years,
        CASE
            WHEN tenure_years_current IS NULL THEN 'Unknown'
            WHEN tenure_years_current < 1     THEN '<1 year'
            WHEN tenure_years_current < 3     THEN '1-2 years'
            WHEN tenure_years_current < 6     THEN '3-5 years'
            WHEN tenure_years_current < 11    THEN '6-10 years'
            ELSE '10+ years'
        END                                                         AS tenure_band,
        silver_loaded_at,
        CURRENT_TIMESTAMP                                           AS gold_loaded_at
    FROM derived
)
SELECT
    driver_key,
    driver_id,
    full_name,
    hire_date,
    termination_date,
    license_number_hash,
    license_state,
    age_years,
    age_band,
    home_terminal,
    employment_status,
    is_active,
    is_employment_consistent,
    cdl_class,
    years_experience,
    experience_band,
    tenure_years,
    tenure_band,
    silver_loaded_at,
    gold_loaded_at
FROM modelled

UNION ALL

SELECT
    {{ gold_unknown_key() }}, '-1', 'Unknown', NULL, NULL, NULL, NULL, NULL, 'Unknown',
    'Unknown', 'Unknown', NULL, NULL, 'Unknown', NULL, 'Unknown', NULL, 'Unknown',
    NULL, CURRENT_TIMESTAMP
