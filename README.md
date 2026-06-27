# 🎓 Student Health Analytics Pipeline — Byte Busters
### DEPI R4 Capstone Project

> An end-to-end data engineering pipeline that ingests, transforms, and serves student health analytics using industry-standard tools: PySpark, DuckDB, dbt, and Apache Airflow — all orchestrated in Docker.

---

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture & Tools Used](#architecture--tools-used)
3. [Pipeline Stages](#pipeline-stages)
4. [Project Structure](#project-structure)
5. [Team Setup Guide — How to Run This](#team-setup-guide)
6. [Running the Pipeline](#running-the-pipeline)
7. [Opening Airflow](#opening-airflow)

---

## Project Overview

This pipeline processes a dataset of 5,000+ student health records across 7 CSV files. It validates, ingests, transforms, and loads the data into a star-schema-ready DuckDB warehouse, with dbt-powered dimensional models and Airflow orchestration.

**Team:** Byte Busters — DEPI R4 Cohort

---

## Architecture & Tools Used

| Layer | Tool | Version | Purpose |
|---|---|---|---|
| Orchestration | Apache Airflow | 2.7.1 | Schedules and monitors all pipeline stages |
| Containerization | Docker + Docker Compose | Latest | Reproducible environment for every teammate |
| Ingestion | PySpark | 4.1.2 | Distributed CSV ingestion and schema validation |
| Storage / Warehouse | DuckDB | 1.5.x | Local analytical warehouse |
| Transformation | dbt-core | 1.8.x | SQL models: staging → intermediate → marts |
| dbt Adapter (DuckDB) | dbt-duckdb | 1.8.x | dbt ↔ DuckDB connector |
| dbt Adapter (SQL Server) | dbt-sqlserver | 1.8.x | dbt ↔ SQL Server connector |
| SQL Server Load | pyodbc | 4.x | Stage 5 load into Azure SQL / SQL Server |
| Data Quality | Great Expectations | Built-in | Stage 6 validation suite |
| Version Control | GitHub | — | Source of truth for all code |

---

## Pipeline Stages

```
Stage 1 → Stage 2 → Stage 3 → Stage 4 → Stage 5 → Stage 6
```

| Stage | Name | Tool | What it does |
|---|---|---|---|
| **Stage 1** | Pre-flight Validation | Python | Checks all 7 source CSV files exist and are non-empty |
| **Stage 2** | Spark Ingest | PySpark | Reads CSVs, enforces schema, writes Parquet |
| **Stage 3** | DuckDB Load | DuckDB | Loads Parquet into DuckDB analytical database |
| **Stage 4** | dbt Transform | dbt-core | Runs staging → intermediate → mart SQL models |
| **Stage 5** | SQL Server Load | pyodbc | Pushes final mart tables to SQL Server / Azure SQL |
| **Stage 6** | Data Quality | Great Expectations | Validates row counts, nulls, and ranges |

### dbt Model Layers

```
raw/ (sources.yml)
  └── staging/     (stg_students, stg_health, stg_sleep, stg_activity, stg_diet, stg_fact, stg_date)
        └── intermediate/   (int_student_features — joins all staging models)
              └── marts/    (mrt_bmi_by_gender, mrt_sleep_adequacy, mrt_health_distribution, etc.)
```

---

## Project Structure

```
DEPI_Final_Proj/
├── dags/
│   └── student_health_dag.py        # Airflow DAG — orchestrates all 6 stages
├── pipeline/
│   ├── run_pipeline.py              # Manual runner (runs all stages in order)
│   ├── stage1_validate.py           # Pre-flight checks
│   ├── stage2_spark_ingest.py       # PySpark ingestion
│   ├── stage3_duckdb_load.py        # DuckDB loading
│   ├── stage4_dbt_transform.py      # dbt runner
│   ├── stage5_sqlserver_load.py     # SQL Server loader
│   └── stage6_data_quality.py       # Great Expectations
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                 # dbt connection profiles
│   ├── models/
│   │   ├── raw/sources.yml
│   │   ├── staging/                 # 7 staging models + schema.yml
│   │   ├── intermediate/            # int_student_features.sql
│   │   └── marts/metrics/           # 4 mart models
│   └── macros/                      # bmi_class, pseudonymise, composite_activity_score, etc.
├── data/
│   └── raw/                         # Source CSV files (NOT committed to git — see below)
├── star_schema_source/              # Star schema SQL DDL scripts
├── Dockerfile                       # Custom Airflow image with dbt + DuckDB
├── docker-compose.yml               # Airflow services (webserver, scheduler, init)
├── requirements.txt                 # Python dependencies for Airflow container
└── .gitignore
```

---

## Team Setup Guide

### Prerequisites — Install These First

1. **Docker Desktop** — https://www.docker.com/products/docker-desktop
   - Windows: Enable WSL2 backend in Docker settings
   - Make sure Docker is running before anything else

2. **Git** — https://git-scm.com/downloads

3. **Python 3.10+** (for running the pipeline manually outside Docker) — https://www.python.org/downloads/

---

### Step 1 — Clone the Repository

```bash
git clone https://github.com/<your-username>/DEPI_Final_Proj.git
cd DEPI_Final_Proj
```

---

### Step 2 — Add the Source Data Files

> ⚠️ The CSV files are NOT included in GitHub (they are too large). Your team lead must share them separately.

Place the following 7 CSV files inside `data/raw/`:

```
data/raw/
├── students.csv
├── health_metrics.csv
├── sleep_data.csv
├── activity_data.csv
├── diet_data.csv
├── fact_table.csv
└── dim_date.csv
```

You can share them via **Google Drive**, **OneDrive**, or a **USB drive**.

---

### Step 3 — Start Airflow with Docker

Open a terminal in the project folder and run:

```bash
docker-compose up -d --build
```

This will:
- Build the custom Airflow image with dbt + DuckDB installed
- Start 3 containers: `airflow-init`, `airflow-webserver`, `airflow-scheduler`
- Initialize the Airflow database automatically

Wait about **60–90 seconds** for everything to start.

---

### Step 4 — Open the Airflow Dashboard

Open your browser and go to:

```
http://localhost:8085
```

**Login credentials:**
- Username: `admin`
- Password: `adminpassword`

---

### Step 5 — Run the Pipeline

**Option A — Via Airflow UI (Recommended):**
1. Find the DAG called `student_health_analytics_pipeline`
2. Toggle the blue switch to **Enable** it
3. Click the ▶ **Trigger DAG** button
4. Click the DAG name and switch to **Graph View** to watch stages execute

**Option B — Via Command Line (Manual):**
```bash
# Install dependencies locally first
pip install dbt-core dbt-duckdb duckdb pyodbc

# Run the full pipeline
python pipeline/run_pipeline.py
```

---

### Step 6 — Stop the Containers

When you are done:

```bash
docker-compose down
```

---

## Running the Pipeline

### Manual Run (outside Docker)

```bash
# Run all 6 stages
python pipeline/run_pipeline.py

# Run a specific stage only
python pipeline/stage1_validate.py
python pipeline/stage2_spark_ingest.py
python pipeline/stage3_duckdb_load.py
python pipeline/stage4_dbt_transform.py
python pipeline/stage5_sqlserver_load.py
python pipeline/stage6_data_quality.py
```

### dbt Commands (run from inside dbt_project/)

```bash
cd dbt_project
dbt debug          # Test connection
dbt run            # Run all models
dbt test           # Run all tests
dbt docs generate  # Generate documentation
dbt docs serve     # Open docs in browser (http://localhost:8080)
```

---

## Opening Airflow

If the containers are already running and you just want to reopen the dashboard:

```
http://localhost:8085
```

If containers stopped (e.g. you restarted your PC), just run:

```bash
docker-compose up -d
```

No rebuild needed — the image is already built locally.

---

## Notes for the Team

- **Stage 5 (SQL Server)** and **Stage 6 (Azure)** require credentials. Ask the team lead for the connection string and set it in an `.env` file or update `pipeline/stage5_sqlserver_load.py` directly.
- **dbt profiles** are in `dbt_project/profiles.yml`. Update the DuckDB path or SQL Server credentials if needed.
- The Airflow DAG file is at `dags/student_health_dag.py`. Any changes to the DAG are picked up automatically by the scheduler every 30 seconds.

---

*Built with ❤️ by Byte Busters — DEPI R4 Capstone*
