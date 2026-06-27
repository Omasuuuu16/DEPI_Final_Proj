-- dim_student_transformed.sql
-- Mart model: final pseudonymised student dimension.
-- No raw PII is present in this model.

SELECT
    student_key,
    national_id_hashed,
    first_name,
    last_name,
    gender,
    age_derived,
    start_date,
    end_date,
    is_current
FROM {{ ref('int_student_features') }}
