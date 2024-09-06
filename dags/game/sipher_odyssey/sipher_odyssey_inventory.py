from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators
from airflow.utils.task_group import TaskGroup

BIGQUERY_PROJECT = Variable.get("bigquery_project")

DAG_START_DATE = set_env_value(
    production=datetime(2023, 10, 1), dev=datetime(2023, 12, 22)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
DAG_CATCHUP = set_env_value(production=True, dev=False)

DEFAULT_ARGS = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)

with DAG(
    dag_id="sipher_odyssey_inventory",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["game", "sipher-odyssey", "inventory"],
    catchup=DAG_CATCHUP,
    max_active_runs=1
) as dag:
    
    with TaskGroup(group_id="raw_inventory_balancing_update", prefix_group_id=False) as raw_inventory_balancing_update:

        stg_sipher_server__raw_inventory_balancing_update_today = DbtRunOperator(
            task_id="stg_sipher_server__raw_inventory_balancing_update_today",
            models="stg_sipher_server__raw_inventory_balancing_update_today",
        )

        stg_sipher_server__raw_inventory_balancing_update = DbtRunOperator(
            task_id="stg_sipher_server__raw_inventory_balancing_update",
            models="stg_sipher_server__raw_inventory_balancing_update",
        )
    
    with TaskGroup(group_id="dim_inventory_balancing_update", prefix_group_id=False) as dim_inventory_balancing_update:

        dim_sipher_odyssey_inventory__gear = DbtRunOperator(
            task_id="dim_sipher_odyssey_inventory__gear",
            models="dim_sipher_odyssey_inventory__gear",
        )

        dim_sipher_odyssey_inventory__em_shards = DbtRunOperator(
            task_id="dim_sipher_odyssey_inventory__em_shards",
            models="dim_sipher_odyssey_inventory__em_shards",
        )

        dim_sipher_odyssey_inventory__weapon = DbtRunOperator(
            task_id="dim_sipher_odyssey_inventory__weapon",
            models="dim_sipher_odyssey_inventory__weapon",
        )
    
    with TaskGroup(group_id="int_inventory_balancing_update", prefix_group_id=False) as int_inventory_balancing_update:

        int_user_inventory_balance_new_update_by_day = DbtRunOperator(
            task_id="int_user_inventory_balance_new_update_by_day",
            models="int_user_inventory_balance_new_update_by_day",
            vars={"ds": "{{ ds }}"}
        )

        int_user_inventory_balance_latest_update_by_instance = DbtRunOperator(
            task_id="int_user_inventory_balance_latest_update_by_instance",
            models="int_user_inventory_balance_latest_update_by_instance",
            vars={"ds": "{{ ds }}"}
        )

        int_user_current_inventory_balance = DbtRunOperator(
            task_id="int_user_current_inventory_balance",
            models="int_user_current_inventory_balance",
        )

    with TaskGroup(group_id="fct_inventory_balancing_update", prefix_group_id=False) as fct_inventory_balancing_update:

        fct_sipher_odyssey_user_latest_inventory_balance = DbtRunOperator(
            task_id="fct_sipher_odyssey_user_latest_inventory_balance",
            models="fct_sipher_odyssey_user_latest_inventory_balance",
            vars={"ds": "{{ ds }}"}
        )

        fct_sipher_odyssey_user_current_inventory_balance = DbtRunOperator(
            task_id="fct_sipher_odyssey_user_current_inventory_balance",
            models="fct_sipher_odyssey_user_current_inventory_balance",
        )

        fct_sipher_odyssey_user_last_inventory_balance_by_day = BigQueryInsertJobOperator(
            task_id="fct_sipher_odyssey_user_last_inventory_balance_by_day",
            gcp_conn_id="sipher_gcp",
            configuration={
                "query": {
                    "query": "{% include 'query/fct_sipher_odyssey_user_last_inventory_balance_by_day.sql' %}",  
                    "useLegacySql": False
                }
            },
            params={"bq_project": BIGQUERY_PROJECT},
            dag=dag,
        )
        fct_sipher_odyssey_user_latest_inventory_balance >> fct_sipher_odyssey_user_last_inventory_balance_by_day

raw_inventory_balancing_update >> dim_inventory_balancing_update >> int_inventory_balancing_update >> fct_inventory_balancing_update