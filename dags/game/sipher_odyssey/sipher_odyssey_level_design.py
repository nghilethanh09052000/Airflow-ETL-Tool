from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators
from airflow.utils.task_group import TaskGroup

DAG_START_DATE = set_env_value(
    production=datetime(2023, 10, 26), dev=datetime(2023, 10, 26)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 1 * * *", dev="@once")

DEFAULT_ARGS = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_success",
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)

with DAG(
    dag_id="sipher_odyssey_level_design",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["game", "sipher-odyssey", "level_design"],
    catchup=False,
) as dag:

    with TaskGroup(group_id="check_success_upstream", prefix_group_id=False) as check_success_upstream:

        checksuccess__stg_firebase__sipher_odyssey_events_all_time = ExternalTaskSensor(
            task_id="checksuccess__stg_firebase__sipher_odyssey_events_all_time",
            external_dag_id="dim_sipher_odyssey_player",
            external_task_id="stg_firebase__sipher_odyssey_events_all_time",
            timeout=600,
            check_existence=True,
            execution_delta=timedelta(hours=1)
        )

        checksuccess__int_sipher_odyssey_player_day0_version = ExternalTaskSensor(
            task_id="checksuccess__int_sipher_odyssey_player_day0_version",
            external_dag_id="dim_sipher_odyssey_player",
            external_task_id="int_sipher_odyssey_player_day0_version",
            timeout=600,
            check_existence=True,
            execution_delta=timedelta(hours=1)
        )

        checksuccess__dim_sipher_odyssey_player = ExternalTaskSensor(
            task_id="checksuccess__dim_sipher_odyssey_player",
            external_dag_id="dim_sipher_odyssey_player",
            external_task_id="dim_sipher_odyssey_player",
            timeout=600,
            check_existence=True,
            execution_delta=timedelta(hours=1)
        )

    with TaskGroup(group_id="fct_level_design", prefix_group_id=False) as fct_level_design:

        fct_level_design_gameplay = DbtRunOperator(
            task_id="fct_level_design_gameplay",
            models="fct_level_design_gameplay",
        )

        fct_level_design_lvl = DbtRunOperator(
            task_id="fct_level_design_lvl",
            models="fct_level_design_lvl",
        )
    
    with TaskGroup(group_id="mart_level_design", prefix_group_id=False) as mart_level_design:

        mart_level_design_gameplay = DbtRunOperator(
            task_id="mart_level_design_gameplay",
            models="mart_level_design_gameplay",
        )

        mart_level_design_lvl = DbtRunOperator(
            task_id="mart_level_design_lvl",
            models="mart_level_design_lvl",
        )

        mart_level_design_dungeon = DbtRunOperator(
            task_id="mart_level_design_dungeon",
            models="mart_level_design_dungeon",
        )

        mart_level_design_gameplay >> [mart_level_design_lvl, mart_level_design_dungeon]

    check_success_upstream >> fct_level_design >> mart_level_design
