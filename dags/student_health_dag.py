"""
Apache Airflow DAG for the Student Health Analytics Pipeline
--------------------------------------------------------------------------
This DAG schedules, runs, and monitors Stages 1 to 6 of the analytical pipeline:
  1. Stage 1: Pre-flight CSV validation
  2. Stage 2: Data ingestion (CSV -> Parquet)
  3. Stage 3: DuckDB load (Parquet -> raw schema)
  4. Stage 4a: dbt run (transformations)
  5. Stage 4b: dbt test (data quality validation)
  6. Stage 5: SQL Server load (Transformed serving layer)
  7. Stage 6: SQL Server load (Metrics serving layer)

It uses a mix of PythonOperators (for pipeline python modules) and BashOperators
(for dbt CLI operations) to ensure clean, isolated execution.
"""

import os
import sys
from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

# ── Paths ─────────────────────────────────────────────────────────────────────
# Dynamically locate the project root relative to this DAG file
DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_DIR, ".."))

# Add project root to sys.path so we can import stage modules directly
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ── Pipeline Stage Imports ────────────────────────────────────────────────────
from pipeline import stage1_validate
from pipeline import stage2_spark_ingest
from pipeline import stage3_duckdb_load
from pipeline import stage5_sqlserver_load
from pipeline import stage6_metrics_load

DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "dbt_project")
DBT_PROFILES_DIR = os.path.join(PROJECT_ROOT, "dbt_project")

# ── Default Arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner": "byte_busters",
    "depends_on_past": False,
    "email": ["admin@bytebusters-depi.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),  # SLA alert if pipeline takes > 1 hour
}


# ── Callbacks & Alerting ──────────────────────────────────────────────────────
def on_failure_callback(context):
    """Callback function triggered when any task in the DAG fails."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    log_url = task_instance.log_url

    subject = f"🔴 Airflow Alert: Pipeline Failure in Task '{task_id}'"
    html_content = f"""
    <h3>Student Health Pipeline — Byte Busters Alert</h3>
    <p><b>Task ID:</b> {task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Exception:</b> {exception}</p>
    <p><b>Logs URL:</b> <a href="{log_url}">{log_url}</a></p>
    <br>
    <p>Please resolve the issue and restart the task from the Airflow Web UI.</p>
    """
    send_email(to=default_args["email"], subject=subject, html_content=html_content)


def on_success_callback(context):
    """Callback function triggered when the entire DAG runs successfully."""
    dag_run = context.get("dag_run")
    execution_date = context.get("execution_date")

    subject = "🟢 Airflow Notice: Student Health Pipeline Completed Successfully"
    html_content = f"""
    <h3>Pipeline Succeeded — Byte Busters</h3>
    <p><b>DAG ID:</b> {dag_run.dag_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Run ID:</b> {dag_run.run_id}</p>
    <br>
    <p>All serving tables in <b>StudentHealthDB.Transformed</b> and metrics in 
    <b>StudentHealthDB.Metrics</b> are now fully updated and verified.</p>
    """
    send_email(to=default_args["email"], subject=subject, html_content=html_content)


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="student_health_analytics_pipeline",
    default_args=default_args,
    description="End-to-end Student Health Analytics ELT Pipeline (Byte Busters)",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 26),
    catchup=False,
    max_active_runs=1,
    on_success_callback=on_success_callback,
    tags=["depi", "capstone", "analytics"],
) as dag:

    # ── Task 1: Pre-flight CSV Validation ──────────────────────────────────────
    task_validate_csv = PythonOperator(
        task_id="validate_csv",
        python_callable=stage1_validate.run,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 2: Data Ingestion (CSV -> Parquet) ────────────────────────────────
    task_spark_ingest = PythonOperator(
        task_id="spark_ingest",
        python_callable=stage2_spark_ingest.run,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 3: DuckDB Staging Load ────────────────────────────────────────────
    task_duckdb_load = PythonOperator(
        task_id="duckdb_load",
        python_callable=stage3_duckdb_load.run,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 4a: dbt Run (Transformations) ─────────────────────────────────────
    # Run dbt using the BashOperator inside the target dbt directory
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
        on_failure_callback=on_failure_callback,
    )

    # ── Task 4b: dbt Test (Schema Validation) ──────────────────────────────────
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
        on_failure_callback=on_failure_callback,
    )

    # ── Task 5: SQL Server Load (Transformed schema) ───────────────────────────
    task_sqlserver_load_transformed = PythonOperator(
        task_id="sqlserver_load_transformed",
        python_callable=stage5_sqlserver_load.run,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 6: SQL Server Load (Metrics schema) ───────────────────────────────
    task_sqlserver_load_metrics = PythonOperator(
        task_id="sqlserver_load_metrics",
        python_callable=stage6_metrics_load.run,
        on_failure_callback=on_failure_callback,
    )

    # ══════════════════════════════════════════════════════════════════════════
    # TASK DEPENDENCY MAP
    # ══════════════════════════════════════════════════════════════════════════
    (
        task_validate_csv
        >> task_spark_ingest
        >> task_duckdb_load
        >> task_dbt_run
        >> task_dbt_test
        >> task_sqlserver_load_transformed
        >> task_sqlserver_load_metrics
    )
