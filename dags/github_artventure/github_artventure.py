import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from github_artventure.scripts.github_repo import *
from github_artventure.scripts.github_stars_history import *
from github_artventure.scripts.traffic_info import *
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

start_date = set_env_value(production=datetime(2023, 7, 12), dev=datetime(2023, 7, 12))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="@daily", dev="@once")

default_args = {
    "owner": "nghi.le",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

dag = DAG(
    "github_artventure",
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=2,
    tags=["github", "artventure"],
    catchup=False,
)


repo_clones = PythonOperator(
    task_id="repo_clones",
    dag=dag,
    provide_context=True,
    python_callable=create_repo_clones_table,
)

traffic_views = PythonOperator(
    task_id="traffic_views",
    dag=dag,
    provide_context=True,
    python_callable=create_traffic_views_table,
)

traffic_popular_paths = PythonOperator(
    task_id="traffic_popular_paths",
    dag=dag,
    provide_context=True,
    python_callable=create_traffic_popular_paths_table,
)

traffic_popular_referrers = PythonOperator(
    task_id="traffic_popular_referrers",
    dag=dag,
    provide_context=True,
    python_callable=create_traffic_popular_referrers_table,
)

github_repo = PythonOperator(
    task_id="github_repo",
    dag=dag,
    provide_context=True,
    python_callable=create_github_repo_table,
)

github_stars_history = PythonOperator(
    task_id="github_stars_history",
    dag=dag,
    provide_context=True,
    python_callable=create_repo_stars_history_table,
)

logging.info("Start executing DAG!")
github_stars_history
github_repo
repo_clones
traffic_views
traffic_popular_paths
traffic_popular_referrers
