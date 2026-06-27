-- mrt_weekly_activity.sql
-- Metric: Average health indicators aggregated by calendar week and health condition.
-- Enables temporal trend analysis for population-level health monitoring.

SELECT
    year,
    month,
    CAST(CEIL(EXTRACT(DAY FROM full_date) / 7.0) AS INTEGER)  AS week_of_month,
    health_condition,
    COUNT(*)                                                    AS student_count,
    ROUND(AVG(composite_activity_score),             2)         AS avg_activity_score,
    ROUND(AVG(sleep_duration),                       2)         AS avg_sleep_hours,
    ROUND(AVG(heart_rate),                           1)         AS avg_heart_rate,
    ROUND(AVG(CAST(step_count AS DOUBLE)),           0)         AS avg_steps,
    ROUND(AVG(exercise_duration),                    2)         AS avg_exercise_mins,
    ROUND(AVG(calorie_expenditure),                  0)         AS avg_calories,
    ROUND(AVG(water_intake),                         2)         AS avg_water_intake,
    ROUND(AVG(bmi),                                  2)         AS avg_bmi,
    SUM(CASE WHEN sleep_adequate     THEN 1 ELSE 0 END)         AS adequate_sleep_count,
    SUM(CASE WHEN hydration_adequate THEN 1 ELSE 0 END)         AS adequate_hydration_count,
    ROUND(
        SUM(CASE WHEN sleep_adequate THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2)                                                          AS sleep_adequacy_rate_pct
FROM {{ ref('fact_health_transformed') }}
WHERE full_date IS NOT NULL
GROUP BY year, month, week_of_month, health_condition
ORDER BY year, month, week_of_month, health_condition
