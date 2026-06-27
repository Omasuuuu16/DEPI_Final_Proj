-- stg_activity.sql
-- Staging model: activity type dimension

SELECT
    CAST(activity_key AS BIGINT)  AS activity_key,
    TRIM(Activity_Type)           AS activity_type
FROM {{ source('raw', 'dim_activity') }}
