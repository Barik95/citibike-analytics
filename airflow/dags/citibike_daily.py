"""
citibike_daily.py

Runs once a day at 06:00 UTC:
  1. Rebuild all mart tables (dimensions, facts, aggregations)
  2. Run dbt snapshots (SCD Type 2 — track capacity + operational changes)
  3. Run full dbt test suite
  4. Refresh dbt seeds if CSVs changed

Marts are materialised as tables so this is heavier than the 30-min pipeline —
running it daily keeps costs low while keeping aggregations fresh for dashboards.
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.decorators import dag

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/dbt"

default_args = {
    "owner": "citibike",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="citibike_daily",
    description="Rebuild mart tables, run snapshots, and run full test suite",
    schedule="0 6 * * *",  # 06:00 UTC daily
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["citibike", "dbt", "marts", "snapshots"],
)
def citibike_daily():

    dbt_seeds = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"dbt seed "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"dbt run --select marts "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_snapshots = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"dbt snapshot "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_full",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_seeds >> dbt_marts >> dbt_snapshots >> dbt_test


citibike_daily()
