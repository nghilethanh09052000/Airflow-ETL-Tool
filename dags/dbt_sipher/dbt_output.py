from datetime import datetime, timedelta

from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators

start_date = set_env_value(production=datetime(2023, 9, 3), dev=datetime(2023, 9, 5))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="0 */3 * * *", dev="0 */3 * * *")


DEFAULT_ARGS = {
    "owner": "hoang.dang",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    # "on_failure_callback": airflow_callback,

}

DEFAULT_ARGS.update(default_args_for_dbt_operators)

dag = DAG(
    dag_id="dbt_rule_based_test",
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    catchup=False,
    tags=["dbt test", "level_design"],
)

dbt_run = DbtRunOperator(task_id="dbt_run_test", models="marts.level_design.*", dag=dag)

fct_level_design_gameplay = DbtTestOperator(
    task_id="fct_level_design_gameplay", models="fct_level_design_gameplay", dag=dag
)

fct_level_design_lvl = DbtTestOperator(
    task_id="fct_level_design_lvl", models="fct_level_design_lvl", dag=dag
)

mart_level_design_gameplay = DbtTestOperator(
    task_id="mart_level_design_gameplay", models="mart_level_design_gameplay", dag=dag
)

mart_level_design_lvl = DbtTestOperator(
    task_id="mart_level_design_lvl", models="mart_level_design_lvl", dag=dag
)
 

dbt_run >> fct_level_design_gameplay >> fct_level_design_lvl >>  mart_level_design_gameplay >> mart_level_design_lvl
 

