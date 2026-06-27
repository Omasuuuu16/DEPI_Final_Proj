"""
Stage 6 — dbt Metrics Promotion to SQL Server (Metrics Schema)
Promotes all 5 metric mart models from DuckDB into SQL Server StudentHealthDB.Metrics schema.
"""

import os
import sys
import logging
import urllib.parse
import duckdb
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE 6] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DUCKDB_PATH   = os.path.join(BASE_DIR, "data", "student_health.duckdb")
SQL_SERVER    = os.environ.get("SQL_SERVER_HOST", "localhost")
DATABASE      = os.environ.get("SQL_SERVER_DB", "StudentHealthDB")
SQL_USER      = os.environ.get("SQL_SERVER_USER")
SQL_PASSWORD  = os.environ.get("SQL_SERVER_PASSWORD")
TARGET_SCHEMA = "Metrics"

# All 5 governed metric tables
METRIC_TABLES = [
    "mrt_health_distribution",
    "mrt_bmi_by_gender",
    "mrt_sleep_adequacy",
    "mrt_weekly_activity",
    "mrt_risk_correlations",
]

DUCKDB_SCHEMA = "metrics"
CHUNK_SIZE    = 1_000


def get_odbc_driver() -> str:
    """Detect the best available SQL Server ODBC driver."""
    available = pyodbc.drivers()
    for preferred in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]:
        if preferred in available:
            return preferred
    matches = [d for d in available if "SQL Server" in d]
    if matches:
        return matches[0]
    raise RuntimeError(f"No SQL Server ODBC driver found. Available: {available}")


def build_engine(driver: str, trust_cert: bool = True) -> object:
    """Build SQLAlchemy engine supporting both SQL auth and Windows Auth."""
    cert_option = ";TrustServerCertificate=yes" if trust_cert else ""
    if SQL_USER and SQL_PASSWORD:
        auth_part = f"UID={SQL_USER};PWD={SQL_PASSWORD};"
    else:
        auth_part = "Trusted_Connection=yes;"

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};SERVER={SQL_SERVER};DATABASE={DATABASE};"
        f"{auth_part}{cert_option}"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
    )


def create_schema_if_not_exists(engine, schema: str) -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'{schema}') "
                f"EXEC('CREATE SCHEMA [{schema}]');"
            )
        )
        conn.commit()
    log.info(f"✅  Schema '{schema}' ready in {DATABASE}.")


def promote_metric_table(
    duckdb_conn: duckdb.DuckDBPyConnection,
    engine,
    table_name: str,
) -> bool:
    log.info(f"Promoting {DUCKDB_SCHEMA}.{table_name} → {TARGET_SCHEMA}.{table_name} …")
    try:
        df = duckdb_conn.execute(
            f"SELECT * FROM {DUCKDB_SCHEMA}.{table_name}"
        ).df()
        rows = len(df)
        log.info(f"    Read {rows:,} rows from DuckDB.")

        # Convert bool → int for SQL Server
        for col in df.select_dtypes(include="bool").columns:
            df[col] = df[col].astype(int)

        df.to_sql(
            table_name,
            engine,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=CHUNK_SIZE,
        )
        log.info(f"✅  Metrics.{table_name}: {rows:,} rows → SQL Server.")
        return True

    except Exception as exc:
        log.error(f"❌  Failed to promote metric '{table_name}': {exc}")
        return False


def print_metric_summary(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Print a quick preview of each metric table."""
    log.info("-" * 60)
    log.info("METRIC TABLES SUMMARY:")
    for table in METRIC_TABLES:
        try:
            row_count = duckdb_conn.execute(
                f"SELECT COUNT(*) FROM {DUCKDB_SCHEMA}.{table}"
            ).fetchone()[0]
            log.info(f"    {table:40s} {row_count:>6,} rows")
        except Exception:
            log.warning(f"    {table:40s} — could not query")


def run():
    log.info("=" * 60)
    log.info("  STAGE 6 — dbt METRICS → SQL SERVER (Metrics Schema)")
    log.info("=" * 60)

    driver = get_odbc_driver()
    log.info(f"ODBC Driver: {driver}")

    engine = build_engine(driver)
    create_schema_if_not_exists(engine, TARGET_SCHEMA)

    if not os.path.exists(DUCKDB_PATH):
        log.error(f"❌  DuckDB file not found: {DUCKDB_PATH}")
        sys.exit(1)

    duckdb_conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    all_passed = True

    try:
        print_metric_summary(duckdb_conn)
        log.info("-" * 60)

        for table_name in METRIC_TABLES:
            ok = promote_metric_table(duckdb_conn, engine, table_name)
            if not ok:
                all_passed = False
    finally:
        duckdb_conn.close()
        engine.dispose()

    log.info("-" * 60)
    if all_passed:
        log.info(
            f"✅  Stage 6 PASSED — all metric tables promoted to {DATABASE}.{TARGET_SCHEMA}"
        )
    else:
        log.error("❌  Stage 6 FAILED — check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    run()
