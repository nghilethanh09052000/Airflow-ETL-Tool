from datetime import datetime

from airflow import DAG
from airflow import settings
from airflow.decorators import task
from alerting.scripts.check_gcp_cost import \
    check_daily_cost_is_not_larger_than_double_avg_7d
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

QUERY_PATH = settings.DAGS_FOLDER + "/alerting/query"

start_date = set_env_value(production=datetime(2022, 8, 2), dev=datetime(2022, 8, 17))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="0 10 * * *", dev="@once")

default_args = {
    "owner": "nghi.le",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}


with DAG(
    dag_id="infrastructure_cost_alert",
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    template_searchpath=QUERY_PATH
) as dag:

    @task(templates_exts=[".sql"])
    def check_daily_cost_is_not_larger_than_double_avg_7d_task(query):
        check_daily_cost_is_not_larger_than_double_avg_7d(query)

    check_daily_cost_is_not_larger_than_double_avg_7d_task(query="check_daily_cost_is_not_larger_than_double_avg_7d.sql")
