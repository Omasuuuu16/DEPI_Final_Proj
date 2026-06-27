"""
Stage 5 — SQL Server Serving Layer (Transformed Schema)
Promotes dbt mart models from DuckDB into SQL Server StudentHealthDB.Transformed schema.
Uses Windows Authentication (trusted connection).
"""

import os
import sys
import logging
import urllib.parse
import duckdb
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE 5] %(levelname)s — %(message)s",
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
TARGET_SCHEMA = "Transformed"

# Tables to promote: (duckdb_schema, duckdb_table, sqlserver_table)
TABLES = [
    ("marts", "dim_student_transformed",  "dim_student"),
    ("marts", "fact_health_transformed",  "fact_health_analytics"),
]

CHUNK_SIZE = 2_000


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
    # Fallback: first driver that mentions SQL Server
    matches = [d for d in available if "SQL Server" in d]
    if matches:
        return matches[0]
    raise RuntimeError(
        f"No SQL Server ODBC driver found. Available drivers: {available}"
    )


def create_database_if_not_exists(driver: str) -> None:
    """Create StudentHealthDB on SQL Server if it does not already exist."""
    if SQL_USER and SQL_PASSWORD:
        auth_part = f"UID={SQL_USER};PWD={SQL_PASSWORD};"
    else:
        auth_part = "Trusted_Connection=yes;"

    conn_str = (
        f"DRIVER={{{driver}}};SERVER={SQL_SERVER};DATABASE=master;"
        f"{auth_part}TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{DATABASE}') "
            f"CREATE DATABASE [{DATABASE}];"
        )
        log.info(f"✅  Database '{DATABASE}' ready.")
    finally:
        conn.close()


def create_schema_if_not_exists(engine, schema: str) -> None:
    """Create the target schema within the database."""
    with engine.connect() as conn:
        conn.execute(
            text(
                f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'{schema}') "
                f"EXEC('CREATE SCHEMA [{schema}]');"
            )
        )
        conn.commit()
    log.info(f"✅  Schema '{schema}' ready in {DATABASE}.")


def build_engine(driver: str, trust_cert: bool = True) -> object:
    """Build a SQLAlchemy engine for SQL Server dynamically supporting Windows/SQL auth."""
    cert_option = ";TrustServerCertificate=yes" if trust_cert else ""
    if SQL_USER and SQL_PASSWORD:
        auth_part = f"UID={SQL_USER};PWD={SQL_PASSWORD};"
    else:
        auth_part = "Trusted_Connection=yes;"

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};SERVER={SQL_SERVER};DATABASE={DATABASE};"
        f"{auth_part}{cert_option}"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
    )
    return engine


def promote_table(
    duckdb_conn: duckdb.DuckDBPyConnection,
    engine,
    duckdb_schema: str,
    duckdb_table: str,
    target_table: str,
) -> bool:
    """Read a table from DuckDB and write it to SQL Server."""
    log.info(f"Promoting {duckdb_schema}.{duckdb_table} → {TARGET_SCHEMA}.{target_table} …")
    try:
        df = duckdb_conn.execute(
            f"SELECT * FROM {duckdb_schema}.{duckdb_table}"
        ).df()
        rows = len(df)
        log.info(f"    Read {rows:,} rows from DuckDB.")

        # Convert boolean columns to int (SQL Server doesn't have BOOLEAN)
        bool_cols = df.select_dtypes(include="bool").columns.tolist()
        for col in bool_cols:
            df[col] = df[col].astype(int)

        df.to_sql(
            target_table,
            engine,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=CHUNK_SIZE,
        )
        log.info(f"✅  {TARGET_SCHEMA}.{target_table}: {rows:,} rows written to SQL Server.")
        return True

    except Exception as exc:
        log.error(f"❌  Failed to promote {duckdb_table}: {exc}")
        return False


def run():
    log.info("=" * 60)
    log.info("  STAGE 5 — SQL SERVER LOAD (Transformed Schema)")
    log.info("=" * 60)

    driver = get_odbc_driver()
    log.info(f"ODBC Driver: {driver}")

    # 1. Create database & schema
    create_database_if_not_exists(driver)
    engine = build_engine(driver)
    create_schema_if_not_exists(engine, TARGET_SCHEMA)

    # 2. Open DuckDB
    if not os.path.exists(DUCKDB_PATH):
        log.error(f"❌  DuckDB file not found: {DUCKDB_PATH}")
        sys.exit(1)
    duckdb_conn = duckdb.connect(DUCKDB_PATH, read_only=True)

    all_passed = True
    try:
        for duckdb_schema, duckdb_table, target_table in TABLES:
            ok = promote_table(duckdb_conn, engine, duckdb_schema, duckdb_table, target_table)
            if not ok:
                all_passed = False
    finally:
        duckdb_conn.close()
        engine.dispose()

    log.info("-" * 60)
    if all_passed:
        log.info(f"✅  Stage 5 PASSED — all tables promoted to {DATABASE}.{TARGET_SCHEMA}")
    else:
        log.error("❌  Stage 5 FAILED — check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    run()
