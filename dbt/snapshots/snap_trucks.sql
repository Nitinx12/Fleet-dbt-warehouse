-- Truck attribute history for SCD2 analysis.
{% snapshot snap_trucks %}
{{ config(
    target_schema = 'snapshots',
    unique_key = 'truck_id',
    strategy = 'check',
    check_cols = [
        'unit_number',
        'make',
        'model_year',
        'vin',
        'acquisition_date',
        'acquisition_mileage',
        'fuel_type',
        'tank_capacity_gallons',
        'status',
        'home_terminal'
    ]
) }}

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
    is_available,
    age_years,
    silver_loaded_at
FROM {{ ref('trucks') }}

{% endsnapshot %}
