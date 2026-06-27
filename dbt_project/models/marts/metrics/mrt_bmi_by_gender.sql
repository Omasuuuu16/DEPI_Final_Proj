-- mrt_bmi_by_gender.sql
-- Metric: Average BMI statistics broken down by gender and BMI class.
-- Shows BMI distribution across demographic groups and health conditions.

SELECT
    gender,
    bmi_class,
    health_condition,
    COUNT(*)                                AS record_count,
    ROUND(AVG(bmi),                  2)     AS avg_bmi,
    ROUND(MIN(bmi),                  2)     AS min_bmi,
    ROUND(MAX(bmi),                  2)     AS max_bmi,
    ROUND(STDDEV(bmi),               2)     AS stddev_bmi,
    ROUND(AVG(composite_activity_score), 2) AS avg_activity_score,
    ROUND(AVG(age_derived),          1)     AS avg_age,
    ROUND(AVG(calorie_expenditure),  0)     AS avg_calories,
    SUM(CASE WHEN sleep_adequate THEN 1 ELSE 0 END) AS sleep_adequate_count
FROM {{ ref('fact_health_transformed') }}
GROUP BY gender, bmi_class, health_condition
ORDER BY gender, bmi_class, health_condition
