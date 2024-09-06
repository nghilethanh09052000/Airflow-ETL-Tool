from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.utils.task_group import TaskGroup

from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators

DAG_START_DATE = set_env_value(
    production=datetime(2023, 8, 23), dev=datetime(2023, 4, 10)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 */12 * * *", dev="@once")

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
    dag_id="dim_sipher_odyssey_player",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["game", "sipher-odyssey"],
    catchup=False,
) as dag:

    with TaskGroup(group_id="stg_firebase_sipher_odyssey", prefix_group_id=False) as stg_firebase_sipher_odyssey:

        stg_firebase__sipher_odyssey_events_all_time = DbtRunOperator(
            task_id="stg_firebase__sipher_odyssey_events_all_time",
            models="stg_firebase__sipher_odyssey_events_all_time",
        )

        stg_firebase__sipher_odyssey_events_14d = DbtRunOperator(
            task_id="stg_firebase__sipher_odyssey_events_14d",
            models="stg_firebase__sipher_odyssey_events_14d",
        )

        stg_firebase__sipher_odyssey_events_3d = DbtRunOperator(
            task_id="stg_firebase__sipher_odyssey_events_3d",
            models="stg_firebase__sipher_odyssey_events_3d",
        )

    with TaskGroup(group_id="int_sipher_odyssey_player", prefix_group_id=False) as int_sipher_odyssey_player:

        int_sipher_odyssey_player_devices = DbtRunOperator(
            task_id="int_sipher_odyssey_player_devices",
            models="int_sipher_odyssey_player_devices",
        )

        int_sipher_odyssey_player_geo = DbtRunOperator(
            task_id="int_sipher_odyssey_player_geo",
            models="int_sipher_odyssey_player_geo",
        )

        int_sipher_odyssey_player_version = DbtRunOperator(
            task_id="int_sipher_odyssey_player_version",
            models="int_sipher_odyssey_player_version",
        )

        int_dim_crm_user_info = DbtRunOperator(
            task_id="int_dim_crm_user_info",
            models="int_dim_crm_user_info",
        )

        int_sipher_odyssey_player_day0_version = DbtRunOperator(
            task_id="int_sipher_odyssey_player_day0_version",
            models="int_sipher_odyssey_player_day0_version",
        )

        int_sipher_odyssey_game_user_lifespan = DbtRunOperator(
            task_id="int_sipher_odyssey_game_user_lifespan",
            models="int_sipher_odyssey_game_user_lifespan",
        )

        int_sipher_odyssey_login = DbtRunOperator(
            task_id="int_sipher_odyssey_login",
            models="int_sipher_odyssey_login",
        )

    with TaskGroup(group_id="sipher_odyssey_player", prefix_group_id=False) as sipher_odyssey_player:

        dim_sipher_odyssey_characters = DbtRunOperator(
            task_id="dim_sipher_odyssey_characters",
            models="dim_sipher_odyssey_characters",
        )

        dim_sipher_odyssey_player = DbtRunOperator(
            task_id="dim_sipher_odyssey_player",
            models="dim_sipher_odyssey_player",
        )

    stg_firebase_sipher_odyssey >> int_sipher_odyssey_player >> sipher_odyssey_player


