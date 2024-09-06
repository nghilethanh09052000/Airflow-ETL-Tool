from datetime import datetime, timedelta
from airflow import DAG
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from airflow.models import Variable
from airflow.decorators import task
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from apple_store.scripts.index import (
    get_finance_reports,
    get_sales_reports
)


DAG_START_DATE = set_env_value(
    production=datetime(2024, 3, 8), dev=datetime(2024, 3, 8)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@daily")
GCP_CONN_ID = "sipher_gcp"
DESTINATION_BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
DESTINATION_OBJECT = "apple_store"
BIGQUERY_PROJECT = Variable.get('bigquery_project')
BQ_DATASET = "raw_apple_store"


default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
    "task_concurrency": 3,
    "concurrency": 3,
    "max_active_tasks": 3
}

with DAG(
    dag_id="apple_store",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["apple_store", "raw_reports"],
    catchup=False
) as dag:
    
    partition_expr = "{snapshot_date:DATE}"

    get_finance_reports_task = get_finance_reports(
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        gcs_bucket=DESTINATION_BUCKET,
        gcs_prefix=f"{DESTINATION_OBJECT}/finance_reports",
        gcp_conn_id=GCP_CONN_ID,
        ds= "{{ ds }}",
    )


    get_sales_reports_task = get_sales_reports(
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        gcs_bucket=DESTINATION_BUCKET,
        gcs_prefix=f"{DESTINATION_OBJECT}/sales_reports",
        gcp_conn_id=GCP_CONN_ID,
        ds= "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
    )