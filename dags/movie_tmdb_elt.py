"""
ELT DAG: Run dbt models for movie recommendation project.

This DAG assumes:
- dbt project is located at /opt/airflow/dbt inside the Airflow container
- profiles.yml is also in /opt/airflow/dbt and uses env_var(...) for Snowflake
- Airflow connection 'snowflake_conn' holds Snowflake login, password, account, etc.
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/movie_dbt"

DBT_BIN ="/home/airflow/.local/bin/dbt"

conn = BaseHook.get_connection("snowflake_conn")

default_args = {
    "env": {
        "DBT_USER": conn.login or "",
        "DBT_PASSWORD": conn.password or "",
        "DBT_ACCOUNT": conn.extra_dejson.get("account", ""),
        "DBT_SCHEMA": conn.schema or "",
        "DBT_DATABASE": conn.extra_dejson.get("database", ""),
        "DBT_ROLE": conn.extra_dejson.get("role", ""),
        "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse", ""),
        "DBT_TYPE": "snowflake",
    }
}

with DAG(
    dag_id="movie_dbt_elt",
    start_date=datetime(2025, 3, 19),
    description="Run dbt ELT for movie recommendation models in Snowflake",
    schedule=None,          # you can change to '@daily' etc. later
    catchup=False,
    default_args = default_args, 
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{DBT_BIN} run "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_BIN} test "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_run >> dbt_test 
