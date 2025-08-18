from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id = 'sleet_snowflake',
        profile_args = {'database':'sleet_oms', 'schema':'landing'}))

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/sleet_ols_project"),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule="@weekly",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="sleet_ols_project",
    tags=["dbt", "snowflake", "sleet"],
    doc_md="""
    ### Sleet OLS Project DAG

    This DAG runs dbt models for the Sleet OLS project on Snowflake.
    It uses the Cosmos provider to orchestrate dbt tasks in Airflow.
    """,
)