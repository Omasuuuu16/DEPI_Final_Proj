"""
Stage 3 — DuckDB Local Analytical Staging
Loads Parquet files from Spark output into DuckDB `raw` schema.
"""

import os
import sys
import logging
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE 3] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PARQUET_DIR   = os.path.join(BASE_DIR, "data", "parquet")
DUCKDB_PATH   = os.path.join(BASE_DIR, "data", "student_health.duckdb")

os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

# ── Table definitions ─────────────────────────────────────────────────────────
TABLES = [
    "dim_student",
    "dim_health",
    "dim_lifestyle",
    "dim_activity",
    "dim_sleep",
    "dim_date",
    "fact_health_analytics",
]

EXPECTED_ROWS = 49_998


def run():
    log.info("=" * 60)
    log.info("  STAGE 3 — DUCKDB LOAD (RAW SCHEMA)")
    log.info("=" * 60)
    log.info(f"DuckDB file: {DUCKDB_PATH}")

    con = duckdb.connect(DUCKDB_PATH)
    try:
        # Create raw schema
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        log.info("Schema 'raw' ready.")

        all_passed = True

        for table in TABLES:
            parquet_glob = os.path.join(PARQUET_DIR, table, "*.parquet").replace("\\", "/")

            # Check parquet files exist
            parquet_folder = os.path.join(PARQUET_DIR, table)
            if not os.path.isdir(parquet_folder):
                log.error(f"❌  Parquet folder missing: {parquet_folder}")
                all_passed = False
                continue

            parquet_files = [f for f in os.listdir(parquet_folder) if f.endswith(".parquet")]
            if not parquet_files:
                log.error(f"❌  No .parquet files in: {parquet_folder}")
                all_passed = False
                continue

            # Drop and recreate table
            con.execute(f"DROP TABLE IF EXISTS raw.{table};")
            con.execute(
                f"CREATE TABLE raw.{table} AS "
                f"SELECT * FROM read_parquet('{parquet_glob}');"
            )

            # Row count parity check
            row_count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
            if row_count == EXPECTED_ROWS:
                log.info(f"✅  raw.{table}: {row_count:,} rows loaded — parity OK")
            else:
                log.warning(f"⚠   raw.{table}: {row_count:,} rows (expected {EXPECTED_ROWS:,})")

        # Summary report
        log.info("-" * 60)
        log.info("RAW SCHEMA SUMMARY:")
        tables_info = con.execute(
            "SELECT table_name, estimated_size FROM duckdb_tables() "
            "WHERE schema_name = 'raw' ORDER BY table_name"
        ).fetchall()
        for tname, est_size in tables_info:
            count = con.execute(f"SELECT COUNT(*) FROM raw.{tname}").fetchone()[0]
            log.info(f"    raw.{tname:30s} {count:>7,} rows")

        if all_passed:
            log.info("✅  Stage 3 PASSED — all tables loaded into DuckDB raw schema.")
        else:
            log.error("❌  Stage 3 FAILED — check errors above.")
            sys.exit(1)

    finally:
        con.close()


if __name__ == "__main__":
    run()
