from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import DbtRunOperator


from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators

DAG_START_DATE = set_env_value(
    production=datetime(2023, 8, 23), dev=datetime(2023, 4, 10)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 0 * * *", dev="@once")

DEFAULT_ARGS = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)


with DAG(
    dag_id="dim_hidden_atlas_player",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["game", "hidden-atlas"],
    catchup=False,
) as dag:

    stg_firebase_hidden_atlas_events_all_time = DbtRunOperator(
        task_id="stg_firebase__hidden_atlas_events_all_time",
        models="stg_firebase__hidden_atlas_events_all_time",
    )

    stg_firebase_hidden_atlas_events_14d = DbtRunOperator(
        task_id="stg_firebase__hidden_atlas_events_14d",
        models="stg_firebase__hidden_atlas_events_14d",
    )

    # int_hidden_atlas_player_devices = DbtRunOperator(
    #     task_id="int_hidden_atlas_player_devices",
    #     models="int_hidden_atlas_player_devices",
    # )

    # dim_hidden_atlas_player = DbtRunOperator(
    #     task_id="dim_hidden_atlas_player",
    #     models="dim_hidden_atlas_player",
    # )

    stg_firebase_hidden_atlas_events_14d 
        # >> int_hidden_atlas_player_devices \
        # >> dim_hidden_atlas_player
