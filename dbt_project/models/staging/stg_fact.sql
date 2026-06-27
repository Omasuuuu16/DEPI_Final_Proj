-- stg_fact.sql
-- Staging model: fact table — cast all measures to correct types

SELECT
    CAST(fact_id             AS BIGINT)  AS fact_id,
    CAST(student_key         AS BIGINT)  AS student_key,
    CAST(lifestyle_key       AS BIGINT)  AS lifestyle_key,
    CAST(health_key          AS BIGINT)  AS health_key,
    CAST(activity_key        AS BIGINT)  AS activity_key,
    CAST(sleep_key           AS BIGINT)  AS sleep_key,
    CAST(date_key            AS BIGINT)  AS date_key,
    CAST(bmi                 AS DOUBLE)  AS bmi,
    CAST(heart_rate          AS INTEGER) AS heart_rate,
    CAST(stress_level        AS INTEGER) AS stress_level,
    CAST(sleep_duration      AS DOUBLE)  AS sleep_duration,
    CAST(step_count          AS INTEGER) AS step_count,
    CAST(exercise_duration   AS DOUBLE)  AS exercise_duration,
    CAST(calorie_expenditure AS DOUBLE)  AS calorie_expenditure,
    CAST(water_intake        AS DOUBLE)  AS water_intake
FROM {{ source('raw', 'fact_health_analytics') }}
