from datetime import datetime, timedelta
from airflow import DAG
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from airflow.decorators import task
from airflow.models import Variable

from raw_cost.google_ad_words_cost.tasks import GoogleAdWordsCostTaskGroup
from raw_cost.unity_ad_cost.tasks import UnityAdCostTaskGroup

DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 22), dev=datetime(2023, 9, 1)
)

DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
GCP_CONN_ID = "sipher_gcp"
GCS_PREFIX = "costs"
BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_cost"


default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "trigger_rule": "all_done",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": airflow_callback,
    "task_concurrency": 1,
    "max_active_tasks": 1
}



with DAG(
    dag_id="raw_cost",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["cost", "google", "unity"],
    catchup=False
) as dag:
    

    # UnityAdCostTaskGroup(
    #     group_id="group_tasks_unity_ad_cost",
    #     bq_project=BIGQUERY_PROJECT,
    #     bq_dataset=BQ_DATASET,
    #     gcs_bucket=BUCKET
    # )

    GoogleAdWordsCostTaskGroup(
        group_id="group_tasks_google_ad_words_cost",
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        gcs_bucket=BUCKET,
        gcp_conn_id=GCP_CONN_ID
    )



    