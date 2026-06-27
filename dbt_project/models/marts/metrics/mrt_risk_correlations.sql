-- mrt_risk_correlations.sql
-- Metric: Cross-analysis of behavioural risk factors against health condition outcomes.
-- Identifies which lifestyle patterns most strongly associate with each health category.

SELECT
    health_condition,
    physical_activity_level,
    smoking_alcohol,
    diet_type,
    sleep_quality,
    activity_type,
    COUNT(*)                                                                     AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY health_condition), 2)
                                                                                 AS pct_within_condition,
    ROUND(AVG(bmi),                          2)  AS avg_bmi,
    ROUND(AVG(composite_activity_score),     2)  AS avg_activity_score,
    ROUND(AVG(sleep_duration),               2)  AS avg_sleep_hours,
    ROUND(AVG(CAST(stress_level AS DOUBLE)), 2)  AS avg_stress_level,
    ROUND(AVG(water_intake),                 2)  AS avg_water_intake,
    ROUND(AVG(heart_rate),                   1)  AS avg_heart_rate,
    ROUND(AVG(exercise_duration),            2)  AS avg_exercise_mins,
    ROUND(AVG(CAST(step_count AS DOUBLE)),   0)  AS avg_steps,
    SUM(CASE WHEN hydration_adequate THEN 1 ELSE 0 END)  AS hydration_adequate_count,
    SUM(CASE WHEN sleep_adequate     THEN 1 ELSE 0 END)  AS sleep_adequate_count,
    ROUND(
        SUM(CASE WHEN hydration_adequate THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2)                                                       AS hydration_rate_pct,
    ROUND(
        SUM(CASE WHEN sleep_adequate THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2)                                                       AS sleep_adequate_rate_pct
FROM {{ ref('fact_health_transformed') }}
GROUP BY
    health_condition,
    physical_activity_level,
    smoking_alcohol,
    diet_type,
    sleep_quality,
    activity_type
ORDER BY health_condition, record_count DESC
