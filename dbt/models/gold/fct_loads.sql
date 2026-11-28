{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'load_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['load_key'], 'unique': true},
        {'columns': ['load_date_key']},
        {'columns': ['customer_key']},
        {'columns': ['route_key']}
    ],
    tags                 = ['gold', 'fact', 'incremental']
) }}

-- Grain: one row per load.
-- Dimensions: dim_customers, dim_routes, dim_date (load date).
-- Measures are additive (weight, pieces, revenue, surcharges, total_charges).
-- NULL measures stay NULL (unknown), never 0.

WITH loads AS (
    SELECT
        load_id,
        customer_id,
        route_id,
        load_date,
        load_type,
        load_status,
        booking_type,
        weight_lbs,
        pieces,
        revenue,
        fuel_surcharge,
        accessorial_charges,
        total_charges,
        silver_loaded_at
    FROM {{ ref('loads') }}
    {% if is_incremental() %}
    WHERE silver_loaded_at > {{ gold_watermark() }}
    {% endif %}
)
SELECT
    {{ gold_key('load_id') }}                                       AS load_key,
    load_id,

    {{ gold_key('customer_id') }}                                   AS customer_key,
    {{ gold_key('route_id') }}                                      AS route_key,
    {{ gold_date_key('load_date') }}                                AS load_date_key,

    COALESCE(load_type, 'Unknown')                                  AS load_type,
    COALESCE(load_status, 'Unknown')                                AS load_status,
    COALESCE(booking_type, 'Unknown')                               AS booking_type,
    (load_status = 'Cancelled')                                     AS is_cancelled,

    weight_lbs,
    pieces,
    revenue,
    fuel_surcharge,
    accessorial_charges,
    total_charges,

    silver_loaded_at,
    CURRENT_TIMESTAMP                                               AS gold_loaded_at
FROM loads
