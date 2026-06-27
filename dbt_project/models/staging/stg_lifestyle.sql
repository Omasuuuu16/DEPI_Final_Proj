-- stg_lifestyle.sql
-- Staging model: lifestyle dimension

SELECT
    CAST(lifestyle_key AS BIGINT)              AS lifestyle_key,
    LOWER(TRIM(Diet_Type))                     AS diet_type,
    LOWER(TRIM(Smoking_Alcohol))               AS smoking_alcohol,
    LOWER(TRIM(Physical_Activity_Level))       AS physical_activity_level
FROM {{ source('raw', 'dim_lifestyle') }}
