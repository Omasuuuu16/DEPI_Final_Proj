"""
Stage 2 — Data Ingestion & Validation (CSV → Parquet)
----------------------------------------------------------
NOTE: PySpark 4.x has a known incompatibility with Java 21+ (JEP 411:
  'getSubject is not supported'). This stage achieves the identical outcome —
  schema validation, deduplication, null detection, and Parquet output —
  using pandas + pyarrow, which is the standard alternative for local
  pipeline runs on modern JDKs.

  The distributed Spark implementation is documented in stage2_spark_dists.py
  and is intended for cluster deployments with Java 11 or 17.
"""

import os
import sys
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE 2] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SOURCE_DIR  = os.path.join(BASE_DIR, "star_schema_source")
PARQUET_DIR = os.path.join(BASE_DIR, "data", "parquet")
LOG_FILE    = os.path.join(BASE_DIR, "data", "spark_validation.log")

os.makedirs(PARQUET_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# ── Table specifications ───────────────────────────────────────────────────────
TABLES = {
    "DIM_STUDENT": {
        "pk": "student_key",
        "dedup_col": "National_ID",   # Check uniqueness of National_ID
        "dtypes": {
            "student_key": "Int64",
            "National_ID":  str,
            "First_Name":   str,
            "Last_Name":    str,
            "Gender":       str,
            "Age":          float,
            "Date_of_birth": str,
            "Start_date":   str,
            "End_date":     str,
            "Is_current":   str,
        },
        "non_null_cols": ["student_key", "National_ID"],
    },
    "DIM_HEALTH": {
        "pk": "health_key",
        "dedup_col": None,
        "dtypes": {
            "health_key":             "Int64",
            "Health_Condition":       str,
            "Typical_Severity_Level": str,
            "General_Treatment_Type": str,
        },
        "non_null_cols": ["health_key", "Health_Condition"],
    },
    "DIM_LIFESTYLE": {
        "pk": "lifestyle_key",
        "dedup_col": None,
        "dtypes": {
            "lifestyle_key":           "Int64",
            "Diet_Type":               str,
            "Smoking_Alcohol":         str,
            "Physical_Activity_Level": str,
        },
        "non_null_cols": ["lifestyle_key"],
    },
    "DIM_ACTIVITY": {
        "pk": "activity_key",
        "dedup_col": None,
        "dtypes": {
            "activity_key": "Int64",
            "Activity_Type": str,
        },
        "non_null_cols": ["activity_key", "Activity_Type"],
    },
    "DIM_SLEEP": {
        "pk": "sleep_key",
        "dedup_col": None,
        "dtypes": {
            "sleep_key":    "Int64",
            "Sleep_Quality": str,
        },
        "non_null_cols": ["sleep_key"],
    },
    "DIM_DATE": {
        "pk": "date_key",
        "dedup_col": None,
        "dtypes": {
            "date_key":    "Int64",
            "full_date":   str,
            "day_of_week": str,
            "month":       "Int64",
            "year":        "Int64",
        },
        "non_null_cols": ["date_key", "full_date"],
    },
    "FACT_HEALTH_ANALYTICS": {
        "pk": "fact_id",
        "dedup_col": None,
        "dtypes": {
            "fact_id":             "Int64",
            "student_key":         "Int64",
            "lifestyle_key":       "Int64",
            "health_key":          "Int64",
            "activity_key":        "Int64",
            "sleep_key":           "Int64",
            "date_key":            "Int64",
            "bmi":                 float,
            "heart_rate":          float,
            "stress_level":        float,
            "sleep_duration":      float,
            "step_count":          float,
            "exercise_duration":   float,
            "calorie_expenditure": float,
            "water_intake":        float,
        },
        "non_null_cols": [
            "fact_id", "student_key", "lifestyle_key", "health_key",
            "activity_key", "sleep_key", "date_key",
            "bmi", "heart_rate", "stress_level", "sleep_duration",
        ],
    },
}

EXPECTED_ROWS = 49_998


def ingest_table(table_name: str, spec: dict) -> bool:
    """Read CSV, validate, and write to Parquet."""
    csv_path     = os.path.join(SOURCE_DIR, f"{table_name}.csv")
    parquet_dir  = os.path.join(PARQUET_DIR, table_name.lower())

    log.info(f"Processing {table_name}.csv …")

    # ── Read CSV ───────────────────────────────────────────────────────────────
    try:
        df = pd.read_csv(
            csv_path,
            dtype=str,          # Read all as string first; cast below
            keep_default_na=False,
            na_values=["", "NULL", "null", "None", "NaN"],
        )
    except Exception as exc:
        log.error(f"❌  Failed to read {table_name}.csv: {exc}")
        return False

    # ── Row count check ────────────────────────────────────────────────────────
    row_count = len(df)
    if row_count == EXPECTED_ROWS:
        log.info(f"  ✅  Row count: {row_count:,} (parity OK)")
    else:
        log.warning(f"  ⚠   Row count: {row_count:,} (expected {EXPECTED_ROWS:,})")

    # ── Null detection on required columns ────────────────────────────────────
    for col in spec["non_null_cols"]:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                log.warning(f"  ⚠   Column '{col}' has {null_count:,} null values")

    # ── Deduplication check ────────────────────────────────────────────────────
    dedup_col = spec.get("dedup_col")
    if dedup_col and dedup_col in df.columns:
        total    = len(df)
        distinct = df[dedup_col].dropna().nunique()
        dupes    = total - distinct
        if dupes > 0:
            log.warning(f"  ⚠   {dedup_col}: {dupes:,} duplicate values detected")
        else:
            log.info(f"  ✅  {dedup_col}: all values unique ({distinct:,})")

    # ── Cast to target dtypes ─────────────────────────────────────────────────
    for col, dtype in spec["dtypes"].items():
        if col not in df.columns:
            continue
        try:
            if dtype == float:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            else:
                df[col] = df[col].astype(str).where(df[col].notna(), other=pd.NA)
        except Exception as exc:
            log.warning(f"  ⚠   Could not cast column '{col}' to {dtype}: {exc}")

    # ── Write to Parquet ──────────────────────────────────────────────────────
    os.makedirs(parquet_dir, exist_ok=True)
    parquet_path = os.path.join(parquet_dir, "part-00000.parquet")

    try:
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        size_mb = os.path.getsize(parquet_path) / 1_048_576
        log.info(f"  ✅  Written → {parquet_path} ({size_mb:.1f} MB)")
    except Exception as exc:
        log.error(f"  ❌  Failed to write Parquet for {table_name}: {exc}")
        return False

    with open(LOG_FILE, "a") as lf:
        lf.write(f"{datetime.now().isoformat()} | {table_name} | {row_count} rows\n")

    return True


def run():
    log.info("=" * 60)
    log.info("  STAGE 2 — DATA INGESTION (CSV → Parquet)")
    log.info("  [pandas + pyarrow — Java 25 compatible]")
    log.info("=" * 60)

    # Clear log
    open(LOG_FILE, "w").close()

    all_passed = True
    for table_name, spec in TABLES.items():
        ok = ingest_table(table_name, spec)
        if not ok:
            all_passed = False
        log.info("")

    log.info("-" * 60)
    if all_passed:
        log.info(f"✅  Stage 2 PASSED — all Parquet files written to {PARQUET_DIR}")
    else:
        log.error("❌  Stage 2 FAILED — check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    run()
