-- fact_health_transformed.sql
-- Mart model: final feature-engineered fact table.
-- Contains all measures, dimension attributes, and engineered features.
-- No raw PII — student names/national_id are pseudonymised.

SELECT
    -- Keys
    fact_id,
    student_key,
    date_key,

    -- Student demographics (no PII)
    national_id_hashed,
    gender,
    age_derived,
    start_date,
    end_date,

    -- Date attributes
    full_date,
    day_of_week,
    month,
    year,

    -- Health condition
    health_condition,
    severity_level,
    treatment_type,

    -- Lifestyle
    diet_type,
    smoking_alcohol,
    physical_activity_level,
    activity_type,
    sleep_quality,

    -- Raw measures
    bmi,
    heart_rate,
    stress_level,
    sleep_duration,
    step_count,
    exercise_duration,
    calorie_expenditure,
    water_intake,

    -- Engineered features
    bmi_class,
    sleep_adequate,
    hydration_adequate,
    composite_activity_score,
    health_trend_indicator,

    -- Ordinal encoding
    sleep_quality_encoded,
    physical_activity_encoded,
    smoking_alcohol_encoded,

    -- One-hot encoding: diet_type
    diet_veg,
    diet_non_veg,
    diet_balanced,

    -- One-hot encoding: gender
    gender_male,
    gender_female,
    gender_other

FROM {{ ref('int_fact_enriched') }}
