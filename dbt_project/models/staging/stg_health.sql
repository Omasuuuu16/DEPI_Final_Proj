-- stg_health.sql
-- Staging model: health condition dimension

SELECT
    CAST(health_key AS BIGINT)                   AS health_key,
    LOWER(TRIM(Health_Condition))                AS health_condition,
    LOWER(TRIM(Typical_Severity_Level))          AS severity_level,
    TRIM(General_Treatment_Type) AS treatment_type
FROM {{ source('raw', 'dim_health') }}
