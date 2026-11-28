{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'trailer_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['trailer_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types, standardise text, validate values, derive age and availability

WITH source AS (
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
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__trailers') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trailer_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        trailer_id                                              AS trailer_id,
        trailer_number::INTEGER                                 AS trailer_number,
        INITCAP(REGEXP_REPLACE({{ clean_string('trailer_type') }}, '\s+', ' ', 'g'))
                                                                    AS trailer_type,
        length_feet::INTEGER                                    AS length_feet,
        model_year::INTEGER                                     AS model_year,
        UPPER(REGEXP_REPLACE({{ clean_string('vin') }}, '[^A-Z0-9]', '', 'g'))
                                                                    AS vin,
        {{ clean_date('acquisition_date') }}                     AS acquisition_date,
        INITCAP(REGEXP_REPLACE({{ clean_string('status') }}, '[_\s]+', ' ', 'g'))
                                                                    AS status,
        INITCAP(REGEXP_REPLACE({{ clean_string('current_location') }}, '\s+', ' ', 'g'))
                                                                    AS current_location,
        {{ clean_timestamp('loaded_at') }}                       AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        trailer_id,
        trailer_number,
        trailer_type,
        CASE WHEN length_feet >= 0 THEN length_feet END           AS length_feet,
        CASE
            WHEN model_year BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE)::INT
                THEN model_year
        END                                                         AS model_year,
        NULLIF(vin, '')                                            AS vin,
        CASE WHEN acquisition_date <= CURRENT_DATE THEN acquisition_date END AS acquisition_date,
        CASE
            WHEN status IN ('Active', 'Inactive', 'Maintenance')
                THEN status
            ELSE 'Unknown'
        END                                                         AS status,
        current_location,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    trailer_id,
    trailer_number,
    trailer_type,
    length_feet,
    model_year,
    vin,
    acquisition_date,
    status,
    -- only an Active trailer is available for dispatch
    status = 'Active'                                              AS is_available,
    EXTRACT(YEAR FROM CURRENT_DATE)::INT - model_year              AS age_years,
    current_location,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated
