{#- ---------------------------------------------------------------------------
    Gold-layer helpers (Postgres)
--------------------------------------------------------------------------- -#}

{# Surrogate key of the "Unknown" member that every dimension carries. #}
{% macro gold_unknown_key() -%}
    MD5('-1')
{%- endmacro %}


{# Deterministic surrogate key: MD5 of the natural key. A NULL natural key maps to
   the Unknown member, so facts never carry a NULL foreign key. #}
{% macro gold_key(column) -%}
    COALESCE(MD5(CAST({{ column }} AS TEXT)), {{ gold_unknown_key() }})
{%- endmacro %}


{# YYYYMMDD integer key into dim_date. NULL dates map to -1 (Unknown date). #}
{% macro gold_date_key(column) -%}
    COALESCE(TO_CHAR(CAST({{ column }} AS DATE), 'YYYYMMDD')::INTEGER, -1)
{%- endmacro %}


{# One-way salted SHA-256 for PII that must stay joinable but not readable.
   Set the salt with GOLD_PII_SALT; the prod target refuses to hash without one. #}
{% macro gold_hash_pii(column) -%}
    {%- set salt = env_var("GOLD_PII_SALT", "") -%}
    {%- if target.name == "prod" and salt == "" -%}
        {{ exceptions.raise_compiler_error(
            "GOLD_PII_SALT is missing and target is prod: gold_hash_pii refuses to hash PII without a salt."
        ) }}
    {%- endif -%}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        ELSE ENCODE(
            SHA256(CONVERT_TO(CAST({{ column }} AS TEXT) || '{{ salt }}', 'UTF8')),
            'hex'
        )
    END
{%- endmacro %}


{# Mask everything except the last N characters, e.g. ************1234 #}
{% macro gold_mask_tail(column, visible=4) -%}
    CASE
        WHEN {{ column }} IS NULL OR {{ column }} = '' THEN NULL
        WHEN LENGTH({{ column }}) <= {{ visible }} THEN REPEAT('*', LENGTH({{ column }}))
        ELSE REPEAT('*', LENGTH({{ column }}) - {{ visible }}) || RIGHT({{ column }}, {{ visible }})
    END
{%- endmacro %}


{# Incremental watermark for fact tables, read from the fact's own silver_loaded_at.
   The lookback re-processes a small window every run. That is safe because facts
   use delete+insert, and it protects against silver models that commit out of order. #}
{% macro gold_watermark() -%}
    (
        SELECT COALESCE(MAX(silver_loaded_at), '1900-01-01'::TIMESTAMPTZ)
               - INTERVAL '{{ var("gold_lookback_hours", 3) }} hours'
        FROM {{ this }}
    )
{%- endmacro %}
