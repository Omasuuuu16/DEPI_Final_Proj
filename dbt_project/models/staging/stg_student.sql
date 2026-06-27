-- stg_student.sql
-- Staging model: student dimension
-- Casts types, normalises strings, preserves all PII columns (pseudonymisation happens in intermediate layer)

SELECT
    CAST(student_key   AS BIGINT)    AS student_key,
    CAST(National_ID   AS VARCHAR)   AS national_id,
    TRIM(First_Name)                 AS first_name,
    TRIM(Last_Name)                  AS last_name,
    LOWER(TRIM(Gender))              AS gender,
    CAST(Age           AS DOUBLE)    AS age_raw,
    TRY_CAST(Date_of_birth AS DATE)  AS date_of_birth,
    TRY_CAST(Start_date    AS DATE)  AS start_date,
    TRY_CAST(End_date      AS DATE)  AS end_date,
    UPPER(TRIM(Is_current))          AS is_current   -- normalised to 'YES' / 'NO'
FROM {{ source('raw', 'dim_student') }}
