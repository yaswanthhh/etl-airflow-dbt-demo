# ETL + Airflow + dbt + Postgres + Streamlit (Auto-updating Dashboard)

This project demonstrates an end-to-end data pipeline where new CSV “drops” are ingested into Postgres, transformed with dbt into analytics-ready models, and visualized in a Streamlit dashboard that refreshes automatically.

## Architecture (high level)

- **CSV drops (synthetic data)**: A Python generator writes new CSV files into an “incoming” folder at a fixed interval.
- **Orchestration (Airflow)**: An Airflow DAG runs on a schedule (cron) to ingest new files, refresh weather data (optional), and execute dbt transformations. Airflow supports cron expressions via the `schedule` parameter (for example, `*/1 * * * *` runs every minute). [web:278][web:442]
- **Transformations (dbt)**: dbt builds staging + mart/fact models inside Postgres. The `dbt build` command runs models/tests/seeds/snapshots in dependency (DAG) order. [web:449]
- **Dashboard (Streamlit)**: Streamlit queries the final fact/mart table (or view) from Postgres and renders charts/tables. It refreshes itself on a timer and uses caching with a TTL so data doesn’t go stale. Streamlit recommends using `ttl` when caching database calls. [web:448][web:450]

## Project structure (typical)

You may see folders similar to:

- `dags/` – Airflow DAG definitions
- `dbt/auto_analytics/` – dbt project (contains `dbt_project.yml` and `models/`)
- `data/incoming/...` – where CSV drops land
- `data/processed/...` – where ingested files are moved (optional)
- `frontend/` – Streamlit app (e.g., `frontend/app.py`)
- `sql/` – helper SQL files (optional)
- `requirements.txt` – Python dependencies for generator + Streamlit app

## Setup

### Start infrastructure (Docker Compose)
From the repo root:

```bash
docker compose up -d --build
Airflow UI: http://localhost:8080
Postgres: localhost:5432
```
### Createa Python environment
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
### Synthetic CSV generator
```bash
python ./src/generate_csv_drops.py
```
This continuously creates “realistic-looking” CSV batches in data/incoming/... so the pipeline always has new data to ingest.

### Airflow pipeline (Scheduled ingestion + dbt)
#### Schedule the DAG frequently
To simulate “near real-time”, schedule the DAG to run every minute:
```python
schedule="*/1 * * * *"
catchup=False
max_active_runs=1
```
Cron schedules like this are supported by Airflow. [web:278][web:442]

What the DAG typically does
- Detect/ingest new CSV files from data/incoming
- Load raw tables into Postgres
- Run dbt build to create/update staging + mart/fact models [web:449]

Once the DAG has run successfully at least once, the final fact/mart relations will exist in Postgres and the dashboard can query them.

### Streamlit Dashboard (Auto Refresh)
#### Auto-refresh UI
Install streamlit-autorefresh (already in requirements.txt), then in frontend/app.py:

```python
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=10_000, key="data_refresh")  # 10 seconds
```
#### Avoid stale database reads
Cache your DB query function with a short TTL:

```python
import streamlit as st

@st.cache_data(ttl=10)
def load_fact(...):
    ...
```
Streamlit’s caching docs recommend setting a ttl when caching data coming from databases/APIs so results refresh periodically.

#### Run the dashboard
```bash
streamlit run frontend/app.py
```

## Resetting the environment
If you want to start fresh (clear Postgres data, volumes, and generated CSVs), run:

```bash
.\reset_all.ps1
```
This script will:
- Delete CSV files under data/incoming and data/processed
- Stop and remove Docker containers/volumes (Postgres data + Airflow metadata)
- Re-run `docker compose up -d --build` so you can start over cleanly.