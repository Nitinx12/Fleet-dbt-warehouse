-- Customer attribute history for SCD2 analysis.
{% snapshot snap_customers %}
{{ config(
    target_schema = 'snapshots',
    unique_key = 'customer_id',
    strategy = 'check',
    check_cols = [
        'customer_name',
        'customer_type',
        'credit_terms_days',
        'primary_freight_type',
        'account_status',
        'contract_start_date',
        'annual_revenue_potential'
    ]
) }}

SELECT
    customer_id,
    customer_name,
    customer_type,
    credit_terms_days,
    primary_freight_type,
    account_status,
    contract_start_date,
    annual_revenue_potential,
    silver_loaded_at
FROM {{ ref('customers') }}

{% endsnapshot %}
