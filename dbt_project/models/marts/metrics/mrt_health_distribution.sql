-- mrt_health_distribution.sql
-- Metric: Distribution of health conditions by gender and diet type.
-- Addresses class imbalance by surfacing minority class behaviour explicitly.

SELECT
    health_condition,
    gender,
    diet_type,
    COUNT(*)                                                             AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),                2)  AS pct_of_total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY health_condition), 2)
                                                                         AS pct_within_condition,
    ROUND(AVG(composite_activity_score), 2)                              AS avg_activity_score,
    ROUND(AVG(bmi),                      2)                              AS avg_bmi,
    ROUND(AVG(sleep_duration),           2)                              AS avg_sleep_hours,
    ROUND(AVG(CAST(stress_level AS DOUBLE)), 2)                          AS avg_stress_level,
    SUM(CASE WHEN sleep_adequate     THEN 1 ELSE 0 END)                  AS sleep_adequate_count,
    SUM(CASE WHEN hydration_adequate THEN 1 ELSE 0 END)                  AS hydration_adequate_count
FROM {{ ref('fact_health_transformed') }}
GROUP BY health_condition, gender, diet_type
ORDER BY health_condition, gender, diet_type
