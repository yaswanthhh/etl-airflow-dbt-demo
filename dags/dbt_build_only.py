from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

CONNECTION_ID = "warehouse_postgres"

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/dbt/auto_analytics"
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

def prepare_dbt_profiles_dir():
    os.makedirs(DBT_PROFILES_DIR, exist_ok=True)

with DAG(
    dag_id="dbt_build_only",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
) as dag:

    prep = PythonOperator(
        task_id="prepare_dbt_profiles_dir",
        python_callable=prepare_dbt_profiles_dir,
    )

    profile_config = ProfileConfig(
        profile_name="auto_analytics",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=CONNECTION_ID,
            profile_args={"schema": "public"},
        ),
    )

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        operator_args={"command": "build"},
    )

    prep >> dbt_build
