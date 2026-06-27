-- int_student_features.sql
-- Intermediate model: student dimension with PII pseudonymisation and age derivation.
-- Filters to current-record SCD snapshot (Is_current = 'YES').

SELECT
    s.student_key,

    -- ── PII Pseudonymisation ───────────────────────────────────────────────
    {{ pseudonymise_id('s.national_id') }}  AS national_id_hashed,
    'PSEUDONYMISED'                         AS first_name,
    'PSEUDONYMISED'                         AS last_name,

    -- ── Demographic ───────────────────────────────────────────────────────
    s.gender,

    -- ── Age derivation from date_of_birth ─────────────────────────────────
    -- Prefer computed age; fall back to source age_raw if DOB is null
    CASE
        WHEN s.date_of_birth IS NOT NULL
             THEN CAST(DATEDIFF('year', s.date_of_birth, CURRENT_DATE) AS INTEGER)
        ELSE CAST(s.age_raw AS INTEGER)
    END AS age_derived,

    -- ── SCD fields ────────────────────────────────────────────────────────
    s.start_date,
    s.end_date,
    s.is_current

FROM {{ ref('stg_student') }} s
WHERE s.is_current = 'YES'
