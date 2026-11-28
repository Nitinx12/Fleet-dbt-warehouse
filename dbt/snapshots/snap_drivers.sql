-- Non-PII driver attribute history for SCD2 analysis.
{% snapshot snap_drivers %}
{{ config(
    target_schema = 'snapshots',
    unique_key = 'driver_id',
    strategy = 'check',
    check_cols = [
        'hire_date',
        'termination_date',
        'license_state',
        'home_terminal',
        'employment_status',
        'cdl_class',
        'years_experience'
    ]
) }}

SELECT
    driver_id,
    hire_date,
    termination_date,
    license_state,
    home_terminal,
    employment_status,
    cdl_class,
    years_experience,
    is_active,
    is_employment_consistent,
    tenure_years,
    CASE
        WHEN age_years IS NULL THEN 'Unknown'
        WHEN age_years < 25    THEN '18-24'
        WHEN age_years < 35    THEN '25-34'
        WHEN age_years < 45    THEN '35-44'
        WHEN age_years < 55    THEN '45-54'
        WHEN age_years < 65    THEN '55-64'
        ELSE '65+'
    END AS age_band,
    silver_loaded_at
FROM {{ ref('drivers') }}

{% endsnapshot %}
