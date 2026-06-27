-- mrt_sleep_adequacy.sql
-- Metric: Sleep adequacy rates across health conditions and sleep quality categories.
-- Supports WHO sleep recommendations analysis.

SELECT
    health_condition,
    sleep_quality,
    sleep_adequate,
    COUNT(*)                                                                           AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY health_condition),  2) AS pct_within_condition,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY sleep_quality),     2) AS pct_within_quality,
    ROUND(AVG(sleep_duration),                  2)  AS avg_sleep_hours,
    ROUND(MIN(sleep_duration),                  2)  AS min_sleep_hours,
    ROUND(MAX(sleep_duration),                  2)  AS max_sleep_hours,
    ROUND(AVG(CAST(stress_level AS DOUBLE)),    2)  AS avg_stress_level,
    ROUND(AVG(composite_activity_score),        2)  AS avg_activity_score,
    ROUND(AVG(heart_rate),                      1)  AS avg_heart_rate
FROM {{ ref('fact_health_transformed') }}
GROUP BY health_condition, sleep_quality, sleep_adequate
ORDER BY health_condition, sleep_quality, sleep_adequate DESC
