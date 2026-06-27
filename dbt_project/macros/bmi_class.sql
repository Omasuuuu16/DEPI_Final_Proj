{#
  Classify BMI into 4 bands per WHO / standard thresholds.
  Usage: {{ bmi_class('bmi') }}  or  {{ bmi_class('f.bmi') }}
#}
{% macro bmi_class(bmi) %}
    CASE
        WHEN CAST({{ bmi }} AS DOUBLE) <  18.5 THEN 'Underweight'
        WHEN CAST({{ bmi }} AS DOUBLE) <  25.0 THEN 'Normal'
        WHEN CAST({{ bmi }} AS DOUBLE) <  30.0 THEN 'Overweight'
        ELSE 'Obese'
    END
{% endmacro %}
