-- stg_date.sql
-- Staging model: date dimension

SELECT
    CAST(date_key      AS BIGINT)   AS date_key,
    TRY_CAST(full_date AS DATE)     AS full_date,
    TRIM(day_of_week)               AS day_of_week,
    CAST(month         AS INTEGER)  AS month,
    CAST(year          AS INTEGER)  AS year
FROM {{ source('raw', 'dim_date') }}
