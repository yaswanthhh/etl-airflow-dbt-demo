from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, glob, shutil

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
INCOMING = f"{AIRFLOW_HOME}/data/incoming/listings"
PROCESSED = f"{AIRFLOW_HOME}/data/processed/listings"

def conn_str():
    return "postgresql://demo:demo@postgres:5432/warehouse"

CREATE_TABLE_SQL = """
create schema if not exists raw;

create table if not exists raw.vehicle_listings (
  listing_id text primary key,
  listing_ts timestamptz not null,
  location text not null,
  make text not null,
  model text not null,
  vehicle_year int not null,
  price_sek numeric not null,
  mileage_km numeric not null,
  source_file text not null,
  ingested_at timestamptz not null default now()
);
"""

def create_table():
    with psycopg2.connect(conn_str()) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)

def load_csv_drop():
    os.makedirs(PROCESSED, exist_ok=True)
    files = sorted(glob.glob(os.path.join(INCOMING, "*.csv")))
    if not files:
        return 0

    total = 0
    with psycopg2.connect(conn_str()) as conn:
        with conn.cursor() as cur:
            for fp in files:
                df = pd.read_csv(fp)
                df["source_file"] = os.path.basename(fp)

                rows = list(df[[
                    "listing_id","listing_ts","location","make","model",
                    "vehicle_year","price_sek","mileage_km","source_file"
                ]].itertuples(index=False, name=None))

                sql = """
                insert into raw.vehicle_listings
                (listing_id, listing_ts, location, make, model, vehicle_year, price_sek, mileage_km, source_file)
                values %s
                on conflict (listing_id) do update set
                  listing_ts = excluded.listing_ts,
                  location = excluded.location,
                  make = excluded.make,
                  model = excluded.model,
                  vehicle_year = excluded.vehicle_year,
                  price_sek = excluded.price_sek,
                  mileage_km = excluded.mileage_km,
                  source_file = excluded.source_file,
                  ingested_at = now()
                """
                execute_values(cur, sql, rows)
                total += len(rows)

                shutil.move(fp, os.path.join(PROCESSED, os.path.basename(fp)))
    return total

with DAG(
    dag_id="ingest_listings_csv",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="create_table", python_callable=create_table)
    t2 = PythonOperator(task_id="ingest_csv_drop", python_callable=load_csv_drop)

    t1 >> t2
