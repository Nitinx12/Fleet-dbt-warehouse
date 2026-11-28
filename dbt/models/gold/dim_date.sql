{{ config(
    materialized = 'table',
    indexes      = [{'columns': ['date_key'], 'unique': true}],
    tags         = ['gold', 'dimension', 'static']
) }}

-- Calendar dimension, one row per day, plus an Unknown member (date_key = -1).
-- Range is controlled by the vars gold_date_start / gold_date_end.
-- Weeks follow ISO 8601 (Monday start).

WITH calendar AS (
    SELECT g.d::DATE AS date_day
    FROM GENERATE_SERIES(
        '{{ var("gold_date_start", "2015-01-01") }}'::DATE,
        '{{ var("gold_date_end",   "2035-12-31") }}'::DATE,
        INTERVAL '1 day'
    ) AS g(d)
),
modelled AS (
    SELECT
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER                      AS date_key,
        date_day,
        EXTRACT(YEAR    FROM date_day)::INT                         AS year_number,
        EXTRACT(QUARTER FROM date_day)::INT                         AS quarter_number,
        'Q' || EXTRACT(QUARTER FROM date_day)::INT                  AS quarter_name,
        EXTRACT(MONTH   FROM date_day)::INT                         AS month_number,
        TO_CHAR(date_day, 'FMMonth')                                AS month_name,
        TO_CHAR(date_day, 'Mon')                                    AS month_short_name,
        TO_CHAR(date_day, 'YYYY-MM')                                AS year_month,
        EXTRACT(ISOYEAR FROM date_day)::INT                         AS iso_year_number,
        EXTRACT(WEEK    FROM date_day)::INT                         AS iso_week_number,
        EXTRACT(DAY     FROM date_day)::INT                         AS day_of_month,
        EXTRACT(DOY     FROM date_day)::INT                         AS day_of_year,
        EXTRACT(ISODOW  FROM date_day)::INT                         AS day_of_week_number,
        TO_CHAR(date_day, 'FMDay')                                  AS day_name,
        EXTRACT(ISODOW FROM date_day) IN (6, 7)                     AS is_weekend,
        DATE_TRUNC('week',  date_day)::DATE                         AS week_start_date,
        DATE_TRUNC('month', date_day)::DATE                         AS month_start_date,
        (DATE_TRUNC('month', date_day) + INTERVAL '1 month - 1 day')::DATE
                                                                    AS month_end_date,
        DATE_TRUNC('quarter', date_day)::DATE                       AS quarter_start_date,
        DATE_TRUNC('year',  date_day)::DATE                         AS year_start_date
    FROM calendar
)
SELECT
    date_key,
    date_day,
    year_number,
    quarter_number,
    quarter_name,
    month_number,
    month_name,
    month_short_name,
    year_month,
    iso_year_number,
    iso_week_number,
    day_of_month,
    day_of_year,
    day_of_week_number,
    day_name,
    is_weekend,
    week_start_date,
    month_start_date,
    month_end_date,
    quarter_start_date,
    year_start_date
FROM modelled

UNION ALL

SELECT
    -1, NULL::DATE, NULL::INT, NULL::INT, 'Unknown', NULL::INT, 'Unknown', 'Unknown', 'Unknown',
    NULL::INT, NULL::INT, NULL::INT, NULL::INT, NULL::INT, 'Unknown', NULL::BOOLEAN,
    NULL::DATE, NULL::DATE, NULL::DATE, NULL::DATE, NULL::DATE
