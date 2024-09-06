from datetime import datetime

from airflow import DAG
from airflow import settings
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

start_date = set_env_value(production=datetime(2023, 8, 1), dev=datetime(2023, 8, 1))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="@daily", dev="@once")

default_args = {
    "owner": "tri.nguyen",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}


with DAG(
    dag_id="adhoc",
    default_args=default_args,
    catchup=False,
    schedule_interval=schedule_interval,
) as dag:

    update_daily_top_apps = BigQueryInsertJobOperator(
        task_id="raw_user_device",
        configuration={
            "query": {
                "query": "{% include 'query/raw_user_device.sql' %}",  
                "useLegacySql": False
            }
        },
        gcp_conn_id="sipher_gcp",
        dag=dag,
    )
    

    update_daily_top_apps