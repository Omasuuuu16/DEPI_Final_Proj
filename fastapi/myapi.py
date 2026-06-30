"""
Student Health Analytics API
Byte Busters — DEPI R4 Capstone Project

Reads from DuckDB mart + metrics layer (dbt output).
Primary tables:
  - marts.fact_health_transformed  → student records
  - marts.dim_student_transformed  → student dimension
  - metrics.mrt_health_distribution
  - metrics.mrt_bmi_by_gender
  - metrics.mrt_risk_correlations
  - metrics.mrt_sleep_adequacy
  - metrics.mrt_weekly_activity
"""

import os
from typing import Optional, List
from contextlib import contextmanager

import duckdb
import httpx
from fastapi import FastAPI, Path, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Student Health Analytics API",
    description="Byte Busters — DEPI R4 | Powered by DuckDB mart & metrics layer.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# DuckDB connection
# ---------------------------------------------------------------------------

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    r"C:/Users/youssef/Desktop/Projects/DEPI_Final_Proj/data/student_health.duckdb",
)

@contextmanager
def get_db():
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        yield conn
    finally:
        conn.close()

def query(sql: str, params: list = []) -> list:
    """Run a query and return list of dicts."""
    try:
        with get_db() as conn:
            result = conn.execute(sql, params)
            columns = [d[0] for d in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class StudentOut(BaseModel):
    fact_id:                  int
    student_key:              Optional[int]   = None
    national_id_hashed:       Optional[str]   = None
    gender:                   Optional[str]   = None
    age_derived:              Optional[int]   = None
    health_condition:         Optional[str]   = None
    severity_level:           Optional[str]   = None
    treatment_type:           Optional[str]   = None
    bmi:                      Optional[float] = None
    bmi_class:                Optional[str]   = None
    heart_rate:               Optional[int]   = None
    sleep_duration:           Optional[float] = None
    sleep_adequate:           Optional[bool]  = None
    sleep_quality:            Optional[str]   = None
    step_count:               Optional[int]   = None
    exercise_duration:        Optional[float] = None
    calorie_expenditure:      Optional[float] = None
    water_intake:             Optional[float] = None
    hydration_adequate:       Optional[bool]  = None
    stress_level:             Optional[int]   = None
    physical_activity_level:  Optional[str]   = None
    activity_type:            Optional[str]   = None
    diet_type:                Optional[str]   = None
    smoking_alcohol:          Optional[str]   = None
    composite_activity_score: Optional[float] = None
    health_trend_indicator:   Optional[str]   = None
    full_date:                Optional[str]   = None

class PipelineRunResponse(BaseModel):
    status:  str
    message: str
    dag_id:  Optional[str] = None

# ---------------------------------------------------------------------------
# Airflow settings
# ---------------------------------------------------------------------------

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080/api/v1")
AIRFLOW_USER     = os.getenv("AIRFLOW_USER",     "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

# ---------------------------------------------------------------------------
# Root + Health check
# ---------------------------------------------------------------------------

@app.get("/", tags=["General"])
def index():
    return {
        "project": "Student Health Analytics Pipeline",
        "team":    "Byte Busters",
        "layer":   "DuckDB marts + metrics (dbt output)",
        "docs":    "/docs",
    }

@app.get("/health", tags=["General"])
def health_check():
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "database": "duckdb", "path": DUCKDB_PATH}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DuckDB unreachable: {str(e)}")

@app.get("/tables", tags=["General"], summary="List all tables in DuckDB")
def list_tables():
    return query("SHOW ALL TABLES")

# ---------------------------------------------------------------------------
# Students — marts.fact_health_transformed
# ---------------------------------------------------------------------------

@app.get(
    "/students",
    response_model=List[StudentOut],
    tags=["Students"],
    summary="List students from the mart layer with filters",
)
def list_students(
    health_condition: Optional[str] = Query(None, description="fit | at-risk | unhealthy"),
    gender:           Optional[str] = Query(None, description="male | female | other"),
    bmi_class:        Optional[str] = Query(None, description="Underweight | Normal | Overweight | Obese"),
    diet_type:        Optional[str] = Query(None, description="veg | non-veg | balanced"),
    limit:            int           = Query(50,   ge=1, le=500),
    offset:           int           = Query(0,    ge=0),
):
    conditions = ["1=1"]
    params: list = []

    if health_condition:
        conditions.append("health_condition = ?")
        params.append(health_condition)
    if gender:
        conditions.append("gender = ?")
        params.append(gender)
    if bmi_class:
        conditions.append("bmi_class = ?")
        params.append(bmi_class)
    if diet_type:
        conditions.append("diet_type = ?")
        params.append(diet_type)

    where = " AND ".join(conditions)
    params += [limit, offset]

    return query(
        f"""
        SELECT
            fact_id, student_key, national_id_hashed, gender, age_derived,
            health_condition, severity_level, treatment_type,
            bmi, bmi_class, heart_rate, sleep_duration, sleep_adequate,
            sleep_quality, step_count, exercise_duration, calorie_expenditure,
            water_intake, hydration_adequate, stress_level,
            physical_activity_level, activity_type, diet_type, smoking_alcohol,
            composite_activity_score, health_trend_indicator,
            CAST(full_date AS VARCHAR) AS full_date
        FROM marts.fact_health_transformed
        WHERE {where}
        ORDER BY fact_id
        LIMIT ? OFFSET ?
        """,
        params,
    )


@app.get(
    "/students/{fact_id}",
    response_model=StudentOut,
    tags=["Students"],
    summary="Get a single student record by fact_id",
)
def get_student(fact_id: int = Path(..., gt=0)):
    rows = query(
        """
        SELECT
            fact_id, student_key, national_id_hashed, gender, age_derived,
            health_condition, severity_level, treatment_type,
            bmi, bmi_class, heart_rate, sleep_duration, sleep_adequate,
            sleep_quality, step_count, exercise_duration, calorie_expenditure,
            water_intake, hydration_adequate, stress_level,
            physical_activity_level, activity_type, diet_type, smoking_alcohol,
            composite_activity_score, health_trend_indicator,
            CAST(full_date AS VARCHAR) AS full_date
        FROM marts.fact_health_transformed
        WHERE fact_id = ?
        """,
        [fact_id],
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"fact_id {fact_id} not found.")
    return rows[0]

# ---------------------------------------------------------------------------
# Metrics — all five metrics mart tables
# ---------------------------------------------------------------------------

@app.get(
    "/metrics/health-distribution",
    tags=["Metrics"],
    summary="Health condition distribution with averages (mrt_health_distribution)",
)
def health_distribution(
    health_condition: Optional[str] = Query(None),
    gender:           Optional[str] = Query(None),
    diet_type:        Optional[str] = Query(None),
):
    conditions = ["1=1"]
    params = []
    if health_condition:
        conditions.append("health_condition = ?")
        params.append(health_condition)
    if gender:
        conditions.append("gender = ?")
        params.append(gender)
    if diet_type:
        conditions.append("diet_type = ?")
        params.append(diet_type)

    return query(
        f"""
        SELECT * FROM metrics.mrt_health_distribution
        WHERE {" AND ".join(conditions)}
        ORDER BY record_count DESC
        """,
        params,
    )


@app.get(
    "/metrics/class-balance",
    tags=["Metrics"],
    summary="Class imbalance — count and % per health condition",
)
def class_balance():
    return query(
        """
        SELECT
            health_condition,
            SUM(record_count)  AS total_records,
            ROUND(SUM(pct_of_total), 2) AS pct_of_total
        FROM metrics.mrt_health_distribution
        GROUP BY health_condition
        ORDER BY total_records DESC
        """
    )


@app.get(
    "/metrics/bmi-by-gender",
    tags=["Metrics"],
    summary="BMI breakdown by gender and health condition (mrt_bmi_by_gender)",
)
def bmi_by_gender(
    gender:           Optional[str] = Query(None),
    health_condition: Optional[str] = Query(None),
    bmi_class:        Optional[str] = Query(None),
):
    conditions = ["1=1"]
    params = []
    if gender:
        conditions.append("gender = ?")
        params.append(gender)
    if health_condition:
        conditions.append("health_condition = ?")
        params.append(health_condition)
    if bmi_class:
        conditions.append("bmi_class = ?")
        params.append(bmi_class)

    return query(
        f"""
        SELECT * FROM metrics.mrt_bmi_by_gender
        WHERE {" AND ".join(conditions)}
        ORDER BY health_condition, bmi_class
        """,
        params,
    )


@app.get(
    "/metrics/risk-correlations",
    tags=["Metrics"],
    summary="Lifestyle risk factor correlations per health condition (mrt_risk_correlations)",
)
def risk_correlations(
    health_condition:       Optional[str] = Query(None),
    physical_activity_level: Optional[str] = Query(None),
    smoking_alcohol:        Optional[str] = Query(None),
):
    conditions = ["1=1"]
    params = []
    if health_condition:
        conditions.append("health_condition = ?")
        params.append(health_condition)
    if physical_activity_level:
        conditions.append("physical_activity_level = ?")
        params.append(physical_activity_level)
    if smoking_alcohol:
        conditions.append("smoking_alcohol = ?")
        params.append(smoking_alcohol)

    return query(
        f"""
        SELECT * FROM metrics.mrt_risk_correlations
        WHERE {" AND ".join(conditions)}
        ORDER BY record_count DESC
        """,
        params,
    )


@app.get(
    "/metrics/sleep-adequacy",
    tags=["Metrics"],
    summary="Sleep adequacy breakdown per health condition (mrt_sleep_adequacy)",
)
def sleep_adequacy(health_condition: Optional[str] = Query(None)):
    conditions = ["1=1"]
    params = []
    if health_condition:
        conditions.append("health_condition = ?")
        params.append(health_condition)

    return query(
        f"""
        SELECT * FROM metrics.mrt_sleep_adequacy
        WHERE {" AND ".join(conditions)}
        ORDER BY health_condition, sleep_quality
        """,
        params,
    )


@app.get(
    "/metrics/weekly-activity",
    tags=["Metrics"],
    summary="Weekly activity trends per health condition (mrt_weekly_activity)",
)
def weekly_activity(
    health_condition: Optional[str] = Query(None),
    year:             Optional[int] = Query(None),
    month:            Optional[int] = Query(None),
):
    conditions = ["1=1"]
    params = []
    if health_condition:
        conditions.append("health_condition = ?")
        params.append(health_condition)
    if year:
        conditions.append("year = ?")
        params.append(year)
    if month:
        conditions.append("month = ?")
        params.append(month)

    return query(
        f"""
        SELECT * FROM metrics.mrt_weekly_activity
        WHERE {" AND ".join(conditions)}
        ORDER BY year, month, week_of_month
        """,
        params,
    )
