{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'driver_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['driver_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast types, standardise text, validate dates and values
-- Derive full name, age, tenure and employment flags

WITH source AS (
    SELECT
        driver_id,
        first_name,
        last_name,
        hire_date,
        termination_date,
        license_number,
        license_state,
        date_of_birth,
        home_terminal,
        employment_status,
        cdl_class,
        years_experience,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__drivers') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY driver_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
typed AS (
    SELECT
        driver_id                                               AS driver_id,
        INITCAP(REGEXP_REPLACE({{ clean_string('first_name') }}, '\s+', ' ', 'g'))
                                                                    AS first_name,
        INITCAP(REGEXP_REPLACE({{ clean_string('last_name') }}, '\s+', ' ', 'g'))
                                                                    AS last_name,
        {{ clean_date('hire_date') }}                               AS hire_date,
        {{ clean_date('termination_date') }}                        AS termination_date,
        NULLIF(REGEXP_REPLACE(UPPER({{ clean_string('license_number') }}), '[^A-Z0-9]', '', 'g'), '')
                                                                    AS license_number,
        UPPER({{ clean_string('license_state') }})                  AS license_state,
        {{ clean_date('date_of_birth') }}                           AS date_of_birth,
        INITCAP(REGEXP_REPLACE({{ clean_string('home_terminal') }}, '\s+', ' ', 'g'))
                                                                    AS home_terminal,
        INITCAP(REGEXP_REPLACE({{ clean_string('employment_status') }}, '[_\s]+', ' ', 'g'))
                                                                    AS employment_status,
        NULLIF(REGEXP_REPLACE(UPPER({{ clean_string('cdl_class') }}), '^CLASS\s*', ''), '')
                                                                    AS cdl_class,
        years_experience::NUMERIC(4, 1)                             AS years_experience,
        {{ clean_timestamp('loaded_at') }}                          AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        driver_id,
        first_name,
        last_name,
        CASE WHEN hire_date <= CURRENT_DATE THEN hire_date END      AS hire_date,
        -- a termination cannot be in the future or before the hire date
        CASE
            WHEN termination_date <= CURRENT_DATE
             AND (hire_date IS NULL OR termination_date >= hire_date)
                THEN termination_date
        END                                                         AS termination_date,
        license_number,
        CASE WHEN license_state ~ '^[A-Z]{2}$' THEN license_state END AS license_state,
        -- a working driver must be 18 or older, and not implausibly old
        CASE
            WHEN date_of_birth BETWEEN CURRENT_DATE - INTERVAL '90 years'
                                   AND CURRENT_DATE - INTERVAL '18 years'
                THEN date_of_birth
        END                                                         AS date_of_birth,
        home_terminal,
        CASE
            WHEN employment_status IN ('Active', 'Inactive', 'Terminated', 'On Leave')
                THEN employment_status
            ELSE 'Unknown'
        END                                                         AS employment_status,
        CASE WHEN cdl_class IN ('A', 'B', 'C') THEN cdl_class END   AS cdl_class,
        CASE WHEN years_experience BETWEEN 0 AND 60 THEN years_experience END AS years_experience,
        loaded_at,
        _ingested_at
    FROM typed
),
derived AS (
    SELECT
        *,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth))::INT    AS age_years,
        -- tenure runs to termination, or to today for current staff;
        -- a terminated driver with no valid termination date has unknown tenure
        CASE
            WHEN employment_status = 'Terminated' AND termination_date IS NULL THEN NULL
            ELSE ROUND(
                ((COALESCE(termination_date, CURRENT_DATE) - hire_date) / 365.25)::NUMERIC, 2
            )
        END                                                         AS tenure_years
    FROM validated
)
SELECT
    driver_id,
    first_name,
    last_name,
    NULLIF(CONCAT_WS(' ', first_name, last_name), '')               AS full_name,
    hire_date,
    termination_date,
    license_number,
    license_state,
    date_of_birth,
    age_years,
    home_terminal,
    employment_status,
    CASE
        WHEN employment_status = 'Unknown' THEN NULL
        ELSE employment_status = 'Active'
    END                                                             AS is_active,
    -- terminated status and a termination date should always go together
    CASE
        WHEN employment_status = 'Unknown' THEN NULL
        ELSE (employment_status = 'Terminated') = (termination_date IS NOT NULL)
    END                                                             AS is_employment_consistent,
    cdl_class,
    -- cannot have driven for more years than (age - 16)
    CASE
        WHEN age_years IS NULL OR years_experience <= age_years - 16 THEN years_experience
    END                                                             AS years_experience,
    tenure_years,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM derived

