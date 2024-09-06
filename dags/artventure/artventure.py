from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.utils.task_group import TaskGroup
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators

DAG_START_DATE = set_env_value(
    production=datetime(2024, 1, 8), dev=datetime(2024, 1, 8)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")

DEFAULT_ARGS = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_success",
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)


with DAG(
    dag_id="artventure",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["artventure"],
    catchup=False,
) as dag:
    stg_firebase__artventure_events_all_time = DbtRunOperator(
        task_id="stg_firebase__artventure_events_all_time",
        models="stg_firebase__artventure_events_all_time",
    )

    stg_firebase__artventure_events_14d = DbtRunOperator(
        task_id="stg_firebase__artventure_events_14d",
        models="stg_firebase__artventure_events_14d",
    )

    with TaskGroup(group_id="dim", prefix_group_id=False) as dim:
        int_artventure_user_devices = DbtRunOperator(
            task_id="int_artventure_user_devices", 
            models="int_artventure_user_devices"
        )

        dim_artventure_user = DbtRunOperator(
            task_id="dim_artventure_user", 
            models="dim_artventure_user"
        )

        dim_artventure_image = DbtRunOperator(
            task_id="dim_artventure_image",
            models="dim_artventure_image",
        )

        int_artventure_user_devices >> dim_artventure_user

    with TaskGroup(group_id="fact", prefix_group_id=False) as fact:
        fct_artventure_user_events = DbtRunOperator(
            task_id="fct_artventure_user_events", 
            models="fct_artventure_user_events"
        )

        fct_artventure_task_events = DbtRunOperator(
            task_id="fct_artventure_task_events", 
            models="fct_artventure_task_events"
        )

        fct_artventure_image_events = DbtRunOperator(
            task_id="fct_artventure_image_events",
            models="fct_artventure_image_events",
        )
        
        fct_artventure_page_events = DbtRunOperator(
            task_id="fct_artventure_page_events",
            models="fct_artventure_page_events",
        )

    with TaskGroup(group_id="reporting", prefix_group_id=False) as reporting:
        mart_artventure_recipe__usage = DbtRunOperator(
            task_id="mart_artventure_recipe__usage",
            models="mart_artventure_recipe__usage",
        )

        mart_artventure_recipe__feedback = DbtRunOperator(
            task_id="mart_artventure_recipe__feedback",
            models="mart_artventure_recipe__feedback",
        )

        mart_artventure_task = DbtRunOperator(
            task_id="mart_artventure_task",
            models="mart_artventure_task",
        )

        mart_artventure_image_stats = DbtRunOperator(
            task_id="mart_artventure_image_stats",
            models="mart_artventure_image_stats",
        )
        
        mart_artventure_page_stats = DbtRunOperator(
            task_id="mart_artventure_page_stats",
            models="mart_artventure_page_stats",
        )

        tmp_artventure_user_usage_timespent = DbtRunOperator(
            task_id="tmp_artventure_user_usage_timespent",
            models="tmp_artventure_user_usage_timespent",
        )

stg_firebase__artventure_events_all_time >> [dim, fact] >> reporting
