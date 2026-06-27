-- int_fact_enriched.sql
-- Intermediate model: joins all dimensions to the fact table and applies feature engineering.
-- Outputs the fully enriched analytical dataset.

WITH base AS (
    SELECT
        f.fact_id,
        f.student_key,
        f.date_key,

        -- ── Raw measures ──────────────────────────────────────────────────
        f.bmi,
        f.heart_rate,
        f.stress_level,
        f.sleep_duration,
        f.step_count,
        f.exercise_duration,
        f.calorie_expenditure,
        f.water_intake,

        -- ── Health condition (from DIM_HEALTH) ────────────────────────────
        h.health_condition,
        h.severity_level,
        h.treatment_type,

        -- ── Lifestyle (from DIM_LIFESTYLE) ────────────────────────────────
        l.diet_type,
        l.smoking_alcohol,
        l.physical_activity_level,

        -- ── Activity (from DIM_ACTIVITY) ──────────────────────────────────
        a.activity_type,

        -- ── Sleep (from DIM_SLEEP) ────────────────────────────────────────
        sl.sleep_quality,

        -- ── Date (from DIM_DATE) ──────────────────────────────────────────
        d.full_date,
        d.day_of_week,
        d.month,
        d.year,

        -- ── Student demographics (pseudonymised) ──────────────────────────
        st.national_id_hashed,
        st.gender,
        st.age_derived,
        st.start_date,
        st.end_date,

        -- ══════════════════════════════════════════════════════════════════
        -- ENGINEERED FEATURES
        -- ══════════════════════════════════════════════════════════════════

        -- BMI Classification (4-band)
        {{ bmi_class('f.bmi') }}  AS bmi_class,

        -- Sleep adequacy flag (WHO: ≥ 7 hours)
        CASE WHEN f.sleep_duration >= 7.0 THEN TRUE ELSE FALSE END  AS sleep_adequate,

        -- Hydration adequacy flag (≥ 2.0 litres/day)
        CASE WHEN f.water_intake >= 2.0 THEN TRUE ELSE FALSE END     AS hydration_adequate,

        -- Composite Activity Score (0–100)
        {{ composite_activity_score('f.step_count', 'f.exercise_duration', 'f.calorie_expenditure') }}
            AS composite_activity_score,

        -- ── Ordinal Encoding ──────────────────────────────────────────────
        CASE sl.sleep_quality
            WHEN 'poor'    THEN 1
            WHEN 'average' THEN 2
            WHEN 'good'    THEN 3
            ELSE NULL
        END AS sleep_quality_encoded,

        CASE l.physical_activity_level
            WHEN 'sedentary' THEN 1
            WHEN 'moderate'  THEN 2
            WHEN 'active'    THEN 3
            ELSE NULL
        END AS physical_activity_encoded,

        CASE l.smoking_alcohol
            WHEN 'no'         THEN 1
            WHEN 'occasional' THEN 2
            WHEN 'yes'        THEN 3
            ELSE NULL
        END AS smoking_alcohol_encoded,

        -- ── One-hot Encoding: diet_type ───────────────────────────────────
        CASE WHEN l.diet_type = 'veg'      THEN 1 ELSE 0 END AS diet_veg,
        CASE WHEN l.diet_type = 'non-veg'  THEN 1 ELSE 0 END AS diet_non_veg,
        CASE WHEN l.diet_type = 'balanced' THEN 1 ELSE 0 END AS diet_balanced,

        -- ── One-hot Encoding: gender ──────────────────────────────────────
        CASE WHEN st.gender = 'male'   THEN 1 ELSE 0 END AS gender_male,
        CASE WHEN st.gender = 'female' THEN 1 ELSE 0 END AS gender_female,
        CASE WHEN st.gender = 'other'  THEN 1 ELSE 0 END AS gender_other

    FROM {{ ref('stg_fact') }} f
    INNER JOIN {{ ref('stg_health') }}           h  ON f.health_key    = h.health_key
    INNER JOIN {{ ref('stg_lifestyle') }}        l  ON f.lifestyle_key = l.lifestyle_key
    INNER JOIN {{ ref('stg_activity') }}         a  ON f.activity_key  = a.activity_key
    INNER JOIN {{ ref('stg_sleep') }}            sl ON f.sleep_key     = sl.sleep_key
    INNER JOIN {{ ref('stg_date') }}             d  ON f.date_key      = d.date_key
    INNER JOIN {{ ref('int_student_features') }} st ON f.student_key   = st.student_key
)

SELECT
    *,
    -- Health Trend Indicator — derived from composite activity score bands
    CASE
        WHEN composite_activity_score >= 65.0 THEN 'improving'
        WHEN composite_activity_score >= 35.0 THEN 'stable'
        ELSE                                       'declining'
    END AS health_trend_indicator

FROM base
