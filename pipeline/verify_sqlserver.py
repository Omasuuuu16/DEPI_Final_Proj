"""
SQL Server Verification Script
Queries StudentHealthDB on local SQL Server to confirm all tables are populated
with correct rows, schemas, and features.
"""

import urllib.parse
import pyodbc
from sqlalchemy import create_engine, text

SQL_SERVER = "localhost"
DATABASE   = "StudentHealthDB"


def get_odbc_driver() -> str:
    available = pyodbc.drivers()
    for preferred in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]:
        if preferred in available:
            return preferred
    return available[0] if available else None


def run():
    print("=" * 60)
    print("  SQL SERVER DATABASE VERIFICATION")
    print("=" * 60)

    driver = get_odbc_driver()
    if not driver:
        print("❌ No SQL Server ODBC Driver detected!")
        return

    # Build connection
    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};SERVER={SQL_SERVER};DATABASE={DATABASE};"
        "Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    try:
        with engine.connect() as conn:
            # ── 1. Check schemas & tables ─────────────────────────────────────
            print("\n📊 Table List and Row Counts:")
            tables_to_check = [
                ("Transformed", "dim_student"),
                ("Transformed", "fact_health_analytics"),
                ("Metrics", "mrt_health_distribution"),
                ("Metrics", "mrt_bmi_by_gender"),
                ("Metrics", "mrt_sleep_adequacy"),
                ("Metrics", "mrt_weekly_activity"),
                ("Metrics", "mrt_risk_correlations"),
            ]

            for schema, table in tables_to_check:
                try:
                    res = conn.execute(text(f"SELECT COUNT(*) FROM [{schema}].[{table}]")).fetchone()
                    print(f"  ✅  [{schema}].[{table:25s}] : {res[0]:,} rows")
                except Exception as e:
                    print(f"  ❌  [{schema}].[{table:25s}] : Table not found or error ({e})")

            # ── 2. Check PII Pseudonymisation ──────────────────────────────────
            print("\n🛡️  PII Masking Verification (dim_student):")
            res_pii = conn.execute(text(
                "SELECT TOP 3 student_key, national_id_hashed, first_name, last_name, gender, age_derived "
                "FROM Transformed.dim_student"
            )).fetchall()
            for r in res_pii:
                print(
                    f"  Key: {r[0]} | "
                    f"NationalID (Hashed): {r[1][:8]}... | "
                    f"First Name: {r[2]} | "
                    f"Last Name: {r[3]} | "
                    f"Gender: {r[4]} | "
                    f"Age (Derived): {r[5]}"
                )

            # ── 3. Check Feature Engineering ───────────────────────────────────
            print("\n⚡  Feature Engineering Verification (fact_health_analytics):")
            res_feat = conn.execute(text(
                "SELECT TOP 3 bmi, bmi_class, sleep_adequate, hydration_adequate, "
                "composite_activity_score, health_trend_indicator "
                "FROM Transformed.fact_health_analytics"
            )).fetchall()
            for r in res_feat:
                print(
                    f"  BMI: {r[0]} ({r[1]}) | "
                    f"Sleep Adequate: {r[2]} | "
                    f"Hydration Adequate: {r[3]} | "
                    f"Activity Score: {r[4]} | "
                    f"Trend: {r[5]}"
                )

            # ── 4. Sample metric preview ───────────────────────────────────────
            print("\n📈 Sample Metric Preview (Metrics.mrt_health_distribution):")
            res_metric = conn.execute(text(
                "SELECT TOP 5 health_condition, gender, record_count, avg_activity_score, avg_bmi "
                "FROM Metrics.mrt_health_distribution"
            )).fetchall()
            for r in res_metric:
                print(
                    f"  Condition: {r[0]:10s} | "
                    f"Gender: {r[1]:8s} | "
                    f"Count: {r[2]:5,} | "
                    f"Avg Activity: {r[3]:5.2f} | "
                    f"Avg BMI: {r[4]:5.2f}"
                )

    except Exception as exc:
        print(f"\n❌ Failed to connect or query SQL Server: {exc}")
    finally:
        engine.dispose()
        print("\n" + "=" * 60)


if __name__ == "__main__":
    run()
