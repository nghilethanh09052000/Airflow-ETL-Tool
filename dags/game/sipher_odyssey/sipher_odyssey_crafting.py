from datetime import datetime
from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators


DAG_START_DATE = set_env_value(
    production=datetime(2023, 12, 15), dev=datetime(2023, 12, 15)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
DEFAULT_ARGS = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)
with DAG(
    dag_id="sipher_odyssey_crafting",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["game", "sipher-odyssey", "crafting"],
    catchup=False,
) as dag:
    
    checksuccess__stg_firebase__sipher_odyssey_events_all_time = ExternalTaskSensor(
        task_id="checksuccess__stg_firebase__sipher_odyssey_events_14d",
        external_dag_id="dim_sipher_odyssey_player",
        external_task_id="stg_firebase__sipher_odyssey_events_14d",
        timeout=600,
        check_existence=True
    )

    fct_sipher_odyssey_crafting = DbtRunOperator(
        task_id="fct_sipher_odyssey_crafting",
        models="fct_sipher_odyssey_crafting",
    )
checksuccess__stg_firebase__sipher_odyssey_events_all_time >> fct_sipher_odyssey_crafting