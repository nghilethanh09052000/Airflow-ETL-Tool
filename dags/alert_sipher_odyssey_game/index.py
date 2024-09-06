from airflow import DAG
from airflow.models import Variable
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from datetime import datetime, timedelta
from alert_sipher_odyssey_game.scripts import send_alerting

DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 22), 
    dev=datetime(2024, 4, 9)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="*/15 * * * *", dev="@once")
GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_billing_project")



default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 8,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_callback,
}


with DAG(
    dag_id="alert_sipher_odyssey_game",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["ALERTING", "SIPHER ODYSSEY"],
    catchup=False
) as dag:

    send_alerting(
        gcp_conn_id=GCP_CONN_ID,
        bigquery_project=BIGQUERY_PROJECT,
        ds= "{{ ds }}"
    )