-- stg_sleep.sql
-- Staging model: sleep quality dimension

SELECT
    CAST(sleep_key AS BIGINT)     AS sleep_key,
    LOWER(TRIM(Sleep_Quality))    AS sleep_quality
FROM {{ source('raw', 'dim_sleep') }}
