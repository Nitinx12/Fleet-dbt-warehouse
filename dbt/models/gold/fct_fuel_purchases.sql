{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = 'fuel_purchase_key',
    on_schema_change     = 'fail',
    indexes              = [
        {'columns': ['fuel_purchase_key'], 'unique': true},
        {'columns': ['purchase_date_key']},
        {'columns': ['driver_key']},
        {'columns': ['truck_key']}
    ],
    tags                 = ['gold', 'fact', 'incremental']
) }}

-- Grain: one row per fuel purchase.
-- Dimensions: dim_trucks, dim_drivers, dim_date (purchase date); trip is a degenerate key.
-- PII: fuel_card_number is masked to its last 4 characters.
-- Additive: gallons, total_cost, calculated_total_cost. Non-additive: price_per_gallon
-- (use SUM(total_cost) / SUM(gallons) for an average price).

WITH purchases AS (
    SELECT
        fuel_purchase_id,
        trip_id,
        truck_id,
        driver_id,
        purchase_date,
        location_city,
        location_state,
        fuel_card_number,
        gallons,
        price_per_gallon,
        total_cost,
        calculated_total_cost,
        is_total_cost_consistent,
        silver_loaded_at
    FROM {{ ref('fuel_purchases') }}
    {% if is_incremental() %}
    WHERE silver_loaded_at > {{ gold_watermark() }}
    {% endif %}
)
SELECT
    {{ gold_key('fuel_purchase_id') }}                              AS fuel_purchase_key,
    fuel_purchase_id,
    trip_id,
    {{ gold_key('trip_id') }}                                       AS trip_key,

    {{ gold_key('truck_id') }}                                      AS truck_key,
    {{ gold_key('driver_id') }}                                     AS driver_key,
    {{ gold_date_key('purchase_date') }}                            AS purchase_date_key,

    purchase_date                                                   AS purchase_datetime,
    location_city,
    location_state,
    {{ gold_mask_tail('fuel_card_number') }}                        AS fuel_card_masked,

    gallons,
    price_per_gallon,
    total_cost,
    calculated_total_cost,
    is_total_cost_consistent,

    silver_loaded_at,
    CURRENT_TIMESTAMP                                               AS gold_loaded_at
FROM purchases
