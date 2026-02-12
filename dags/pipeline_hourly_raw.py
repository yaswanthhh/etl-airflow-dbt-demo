from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, glob, shutil
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
INCOMING = f"{AIRFLOW_HOME}/data/incoming/listings"
PROCESSED = f"{AIRFLOW_HOME}/data/processed/listings"

LOCATIONS = {
    "Lund": (55.7047, 13.1910),
    "Malmo": (55.6050, 13.0038),
    "Goteborg": (57.7089, 11.9746),
}
HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m", "weather_code"]

def conn_str():
    return "postgresql://demo:demo@postgres:5432/warehouse"

CREATE_LISTINGS_SQL = """
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

CREATE_WEATHER_SQL = """
create schema if not exists raw;

create table if not exists raw.weather_hourly (
  location text not null,
  ts_hour timestamptz not null,
  temperature_2m numeric,
  precipitation numeric,
  wind_speed_10m numeric,
  weather_code int,
  ingested_at timestamptz not null default now(),
  primary key (location, ts_hour)
);
"""

def create_tables():
    with psycopg2.connect(conn_str()) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_LISTINGS_SQL)
            cur.execute(CREATE_WEATHER_SQL)

def ingest_listings():
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

def ingest_weather():
    frames = []
    for loc, (lat, lon) in LOCATIONS.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(HOURLY_VARS),
            "timezone": "GMT",
            "past_days": 2,
            "forecast_days": 2,
            "timeformat": "iso8601",
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        h = r.json()["hourly"]

        df = pd.DataFrame({
            "ts_hour": pd.to_datetime(h["time"], utc=True),
            "temperature_2m": h.get("temperature_2m"),
            "precipitation": h.get("precipitation"),
            "wind_speed_10m": h.get("wind_speed_10m"),
            "weather_code": h.get("weather_code"),
        })
        df["location"] = loc
        frames.append(df)

    df_all = pd.concat(frames, ignore_index=True)

    rows = list(df_all[[
        "location","ts_hour","temperature_2m","precipitation","wind_speed_10m","weather_code"
    ]].itertuples(index=False, name=None))

    with psycopg2.connect(conn_str()) as conn:
        with conn.cursor() as cur:
            sql = """
            insert into raw.weather_hourly
              (location, ts_hour, temperature_2m, precipitation, wind_speed_10m, weather_code)
            values %s
            on conflict (location, ts_hour) do update set
              temperature_2m = excluded.temperature_2m,
              precipitation = excluded.precipitation,
              wind_speed_10m = excluded.wind_speed_10m,
              weather_code = excluded.weather_code,
              ingested_at = now()
            """
            execute_values(cur, sql, rows)
    return len(rows)

with DAG(
    dag_id="pipeline_hourly_raw",
    start_date=datetime(2026, 2, 1),
    schedule="@hourly",
    catchup=False,
) as dag:
    t0 = PythonOperator(task_id="create_tables", python_callable=create_tables)
    t1 = PythonOperator(task_id="ingest_listings_csv_drop", python_callable=ingest_listings)
    t2 = PythonOperator(task_id="ingest_weather_openmeteo", python_callable=ingest_weather)

    t0 >> [t1, t2]
