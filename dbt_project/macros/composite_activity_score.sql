{#
  Compute a composite activity score on a 0–100 scale.
  Weighted combination of:
    - step_count         (40%) — range: 1,000 – 14,999
    - exercise_duration  (35%) — range: 0 – 120
    - calorie_expenditure(25%) — range: 1,200 – 3,901

  Usage:
    {{ composite_activity_score('step_count', 'exercise_duration', 'calorie_expenditure') }}
    {{ composite_activity_score('f.step_count', 'f.exercise_duration', 'f.calorie_expenditure') }}
#}
{% macro composite_activity_score(step_count, exercise_duration, calorie_expenditure) %}
    ROUND(
        LEAST(100.0, GREATEST(0.0,
            COALESCE(
                (CAST({{ step_count }}          AS DOUBLE) - 1000.0 ) / (14999.0 - 1000.0) * 40.0,
            0.0) +
            COALESCE(
                (CAST({{ exercise_duration }}   AS DOUBLE) -    0.0 ) / (  120.0 -    0.0) * 35.0,
            0.0) +
            COALESCE(
                (CAST({{ calorie_expenditure }} AS DOUBLE) - 1200.0 ) / ( 3901.0 - 1200.0) * 25.0,
            0.0)
        ))
    , 2)
{% endmacro %}
