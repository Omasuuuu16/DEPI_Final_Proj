"""
Master Pipeline Runner — Student Health Analytics Pipeline
Orchestrates all 6 stages sequentially with logging and error handling.

Usage:
    python pipeline/run_pipeline.py
    python pipeline/run_pipeline.py --stage 2   # Run only a specific stage
"""

import os
import sys
import subprocess
import time
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PIPELINE] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DBT_DIR     = os.path.join(BASE_DIR, "dbt_project")
PIPELINE_DIR = os.path.dirname(__file__)

PYTHON = sys.executable


def run_step(label: str, cmd: list, cwd: str = None) -> bool:
    """Run a subprocess step and return True on success."""
    log.info(f"{'─'*60}")
    log.info(f"▶  {label}")
    log.info(f"{'─'*60}")
    start = time.time()

    result = subprocess.run(
        cmd,
        cwd=cwd or BASE_DIR,
        capture_output=False,      # show output live
    )

    elapsed = time.time() - start
    if result.returncode == 0:
        log.info(f"✅  {label} completed in {elapsed:.1f}s")
        return True
    else:
        log.error(f"❌  {label} FAILED (exit code {result.returncode}) after {elapsed:.1f}s")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run Student Health Analytics Pipeline")
    parser.add_argument(
        "--stage", type=int, default=0,
        help="Run only a specific stage (1–6). 0 = run all stages."
    )
    parser.add_argument(
        "--skip-tests", action="store_true",
        help="Skip dbt schema tests (Stage 4b)."
    )
    args = parser.parse_args()

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║    STUDENT HEALTH ANALYTICS PIPELINE — BYTE BUSTERS     ║")
    log.info("║                    DEPI R4 Capstone                     ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    steps = {
        1: ("Stage 1 — Pre-flight Validation",
            [PYTHON, os.path.join(PIPELINE_DIR, "stage1_validate.py")]),
        2: ("Stage 2 — Apache Spark Ingestion (CSV → Parquet)",
            [PYTHON, os.path.join(PIPELINE_DIR, "stage2_spark_ingest.py")]),
        3: ("Stage 3 — DuckDB Load (Parquet → raw schema)",
            [PYTHON, os.path.join(PIPELINE_DIR, "stage3_duckdb_load.py")]),
        4: ("Stage 4a — dbt Run (Transformations)",
            ["dbt", "run", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR]),
        "4b": ("Stage 4b — dbt Test (Schema validation)",
               ["dbt", "test", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR]),
        5: ("Stage 5 — SQL Server Load (Transformed schema)",
            [PYTHON, os.path.join(PIPELINE_DIR, "stage5_sqlserver_load.py")]),
        6: ("Stage 6 — SQL Server Load (Metrics schema)",
            [PYTHON, os.path.join(PIPELINE_DIR, "stage6_metrics_load.py")]),
    }

    pipeline_start = time.time()
    results = {}

    if args.stage != 0:
        # Run single stage
        if args.stage not in steps:
            log.error(f"Unknown stage: {args.stage}. Valid stages: 1–6")
            sys.exit(1)
        label, cmd = steps[args.stage]
        ok = run_step(label, cmd, cwd=BASE_DIR)
        sys.exit(0 if ok else 1)

    # Run full pipeline
    stage_order = [1, 2, 3, 4, "4b", 5, 6]
    if args.skip_tests:
        stage_order.remove("4b")
        log.info("⚠   Skipping dbt schema tests (--skip-tests flag set).")

    for stage_id in stage_order:
        label, cmd = steps[stage_id]
        ok = run_step(label, cmd, cwd=BASE_DIR)
        results[stage_id] = ok
        if not ok:
            log.error(f"Pipeline halted at: {label}")
            log.error("Fix the error above and re-run, or use --stage <N> to resume.")
            break

    total_elapsed = time.time() - pipeline_start

    # Final summary
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║                    PIPELINE SUMMARY                     ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    all_passed = True
    for stage_id, ok in results.items():
        label = steps[stage_id][0]
        icon  = "✅" if ok else "❌"
        log.info(f"  {icon}  {label}")
        if not ok:
            all_passed = False

    log.info(f"\n  Total runtime: {total_elapsed:.1f}s")
    if all_passed:
        log.info("  🎉  ALL STAGES PASSED — Pipeline completed successfully!")
        log.info(f"  📊  Explore results in SQL Server: StudentHealthDB")
        log.info(f"       — StudentHealthDB.Transformed.dim_student")
        log.info(f"       — StudentHealthDB.Transformed.fact_health_analytics")
        log.info(f"       — StudentHealthDB.Metrics.mrt_health_distribution")
        log.info(f"       — StudentHealthDB.Metrics.mrt_bmi_by_gender")
        log.info(f"       — StudentHealthDB.Metrics.mrt_sleep_adequacy")
        log.info(f"       — StudentHealthDB.Metrics.mrt_weekly_activity")
        log.info(f"       — StudentHealthDB.Metrics.mrt_risk_correlations")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
