from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

INCOMING_GLOB = "/opt/airflow/data/incoming/listings/*.csv"

with DAG(
    dag_id="watch_listings_csv_drop",
    start_date=datetime(2026, 2, 1),
    schedule="*/1 * * * *",   # check every minute
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_csv = FileSensor(
        task_id="wait_for_csv",
        filepath=INCOMING_GLOB,
        poke_interval=15,
        timeout=60 * 60,
        mode="reschedule",
    )

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_pipeline_hourly_full",
        trigger_dag_id="pipeline_hourly_full",
        wait_for_completion=False,
    )

    wait_for_csv >> trigger_pipeline
