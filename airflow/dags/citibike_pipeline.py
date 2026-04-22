"""
citibike_pipeline.py

Runs every 30 minutes:
  1. Ingest all 5 CitiBike GBFS endpoints into BigQuery raw tables
  2. Run dbt staging layer  (views — fast, always consistent)
  3. Run dbt intermediate layer (views — fast, always consistent)
  4. Run dbt tests to validate data quality

This DAG keeps raw + staging + intermediate always fresh.
The daily DAG handles the heavier mart aggregations.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt"

default_args = {
    "owner": "citibike",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


@dag(
    dag_id="citibike_pipeline",
    description="Ingest CitiBike GBFS data and refresh staging + intermediate layers",
    schedule="*/30 * * * *",  # every 30 minutes
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["citibike", "ingestion", "dbt"],
)
def citibike_pipeline():

    ingest = BashOperator(
        task_id="ingest_all_endpoints",
        bash_command="python /usr/local/airflow/scripts/ingest_citibike.py",
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"dbt run --select staging "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"dbt run --select intermediate "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --select staging intermediate "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    ingest >> dbt_staging >> dbt_intermediate >> dbt_test


citibike_pipeline()
