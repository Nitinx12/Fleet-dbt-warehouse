{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'facility_id',
    on_schema_change     = 'fail',
    indexes              = [{'columns': ['facility_id'], 'unique': true}],
    tags                 = ['silver', 'incremental']
) }}

-- Extract source data with incremental filtering
-- Remove duplicates, keeping the latest ingested record
-- Cast data types and standardise text

WITH source AS (
    SELECT
        facility_id,
        facility_name,
        facility_type,
        city,
        state,
        latitude,
        longitude,
        dock_doors,
        operating_hours,
        loaded_at,
        _ingested_at
    FROM {{ source('bronze', 'mysql__facilities') }}
    {{ bronze_incremental_filter() }}
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY facility_id
            ORDER BY _ingested_at DESC NULLS LAST, loaded_at DESC NULLS LAST
        ) AS rnk
    FROM source
),
cleaned AS (
    SELECT
        facility_id                                             AS facility_id,
        INITCAP(REGEXP_REPLACE({{ clean_string('facility_name') }}, '\s+', ' ', 'g'))
                                                                    AS facility_name,
        INITCAP(REGEXP_REPLACE({{ clean_string('facility_type') }}, '\s+', ' ', 'g'))
                                                                    AS facility_type,
        INITCAP(REGEXP_REPLACE({{ clean_string('city') }}, '\s+', ' ', 'g'))
                                                                    AS city,
        UPPER({{ clean_string('state') }})                         AS state,
        latitude::NUMERIC(10, 4)                                   AS latitude,
        longitude::NUMERIC(10, 4)                                  AS longitude,
        dock_doors::INTEGER                                        AS dock_doors,
        {{ clean_string('operating_hours') }}                      AS operating_hours,
        {{ clean_timestamp('loaded_at') }}                         AS loaded_at,
        _ingested_at
    FROM deduplicated
    WHERE rnk = 1
),
validated AS (
    SELECT
        facility_id,
        facility_name,
        facility_type,
        city,
        CASE WHEN state ~ '^[A-Z]{2}$' THEN state END              AS state,
        -- a facility sits inside valid geographic bounds
        CASE
            WHEN latitude  BETWEEN -90  AND 90
             AND longitude BETWEEN -180 AND 180
                THEN latitude
        END                                                         AS latitude,
        CASE
            WHEN latitude  BETWEEN -90  AND 90
             AND longitude BETWEEN -180 AND 180
                THEN longitude
        END                                                         AS longitude,
        CASE WHEN dock_doors >= 0 THEN dock_doors END              AS dock_doors,
        operating_hours,
        loaded_at,
        _ingested_at
    FROM cleaned
)
SELECT
    facility_id,
    facility_name,
    facility_type,
    city,
    state,
    latitude,
    longitude,
    dock_doors,
    operating_hours,
    loaded_at,
    _ingested_at,
    CURRENT_TIMESTAMP                                               AS silver_loaded_at
FROM validated
