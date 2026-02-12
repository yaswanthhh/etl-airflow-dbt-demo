from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

LOCATIONS = {
    "Lund": (55.7047, 13.1910),
    "Malmo": (55.6050, 13.0038),
    "Goteborg": (57.7089, 11.9746),
}

HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m", "weather_code"]

def conn_str():
    return "postgresql://demo:demo@postgres:5432/warehouse"

def fetch_one(location, lat, lon):
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
    df["location"] = location
    return df

def load_weather():
    frames = []
    for loc, (lat, lon) in LOCATIONS.items():
        frames.append(fetch_one(loc, lat, lon))
    df = pd.concat(frames, ignore_index=True)

    rows = list(df[[
        "location","ts_hour","temperature_2m","precipitation","wind_speed_10m","weather_code"
    ]].itertuples(index=False, name=None))

    with psycopg2.connect(conn_str()) as conn:
        with conn.cursor() as cur:
            cur.execute(open("/opt/airflow/sql/create_weather_table.sql").read())
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
    dag_id="ingest_weather_openmeteo",
    start_date=datetime(2026, 2, 1),
    schedule=None,   # manual first (simple)
    catchup=False,
) as dag:
    run = PythonOperator(task_id="load_weather", python_callable=load_weather)
