"""
Stage 1 — Pre-flight CSV Validation
Validates all 7 source CSV files before Spark ingestion.
"""

import os
import csv
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE 1] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
SOURCE_DIR = os.path.join(os.path.dirname(__file__), "..", "star_schema_source")

EXPECTED_FILES = {
    "DIM_STUDENT.csv": {
        "columns": [
            "student_key", "National_ID", "First_Name", "Last_Name",
            "Gender", "Age", "Date_of_birth", "Start_date", "End_date", "Is_current",
        ],
        "pii_columns": ["National_ID", "First_Name", "Last_Name", "Date_of_birth"],
        "min_rows": 1,
    },
    "DIM_HEALTH.csv": {
        "columns": ["health_key", "Health_Condition", "Typical_Severity_Level", "General_Treatment_Type"],
        "pii_columns": [],
        "min_rows": 1,
    },
    "DIM_LIFESTYLE.csv": {
        "columns": ["lifestyle_key", "Diet_Type", "Smoking_Alcohol", "Physical_Activity_Level"],
        "pii_columns": [],
        "min_rows": 1,
    },
    "DIM_ACTIVITY.csv": {
        "columns": ["activity_key", "Activity_Type"],
        "pii_columns": [],
        "min_rows": 1,
    },
    "DIM_SLEEP.csv": {
        "columns": ["sleep_key", "Sleep_Quality"],
        "pii_columns": [],
        "min_rows": 1,
    },
    "DIM_DATE.csv": {
        "columns": ["date_key", "full_date", "day_of_week", "month", "year"],
        "pii_columns": [],
        "min_rows": 1,
    },
    "FACT_HEALTH_ANALYTICS.csv": {
        "columns": [
            "fact_id", "student_key", "lifestyle_key", "health_key",
            "activity_key", "sleep_key", "date_key",
            "bmi", "heart_rate", "stress_level", "sleep_duration",
            "step_count", "exercise_duration", "calorie_expenditure", "water_intake",
        ],
        "pii_columns": [],
        "min_rows": 1,
    },
}

EXPECTED_ROW_COUNT = 49_998  # Expected rows in each file


def validate_file(filename: str, spec: dict) -> bool:
    """Validate a single CSV file against its spec."""
    filepath = os.path.join(SOURCE_DIR, filename)

    # 1. File existence
    if not os.path.exists(filepath):
        log.error(f"❌  File not found: {filepath}")
        return False

    # 2. Non-empty check
    if os.path.getsize(filepath) == 0:
        log.error(f"❌  File is empty: {filename}")
        return False

    passed = True
    with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        # 3. Header / column presence
        if header is None:
            log.error(f"❌  No header row in {filename}")
            return False

        # Normalise (strip whitespace, case-insensitive comparison)
        actual_cols = [c.strip() for c in header]
        expected_cols = spec["columns"]

        missing = [c for c in expected_cols if c not in actual_cols]
        if missing:
            log.error(f"❌  {filename}: missing columns → {missing}")
            passed = False

        # 4. PII columns present
        for pii_col in spec.get("pii_columns", []):
            if pii_col not in actual_cols:
                log.error(f"❌  {filename}: PII column '{pii_col}' not found!")
                passed = False

        # 5. Row count
        row_count = sum(1 for _ in reader)
        if row_count < spec["min_rows"]:
            log.error(f"❌  {filename}: has {row_count} rows, expected ≥ {spec['min_rows']}")
            passed = False

        if row_count != EXPECTED_ROW_COUNT:
            log.warning(f"⚠   {filename}: {row_count:,} rows (expected {EXPECTED_ROW_COUNT:,})")
        else:
            log.info(f"✅  {filename}: {row_count:,} rows — columns OK — PII columns present")

    return passed


def run():
    log.info("=" * 60)
    log.info("  STAGE 1 — PRE-FLIGHT CSV VALIDATION")
    log.info("=" * 60)
    log.info(f"Source directory: {os.path.abspath(SOURCE_DIR)}")

    if not os.path.isdir(SOURCE_DIR):
        log.error(f"Source directory does not exist: {SOURCE_DIR}")
        sys.exit(1)

    all_passed = True
    for filename, spec in EXPECTED_FILES.items():
        ok = validate_file(filename, spec)
        if not ok:
            all_passed = False

    log.info("-" * 60)
    if all_passed:
        log.info("✅  Stage 1 PASSED — all files validated successfully.")
    else:
        log.error("❌  Stage 1 FAILED — fix errors above before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    run()
