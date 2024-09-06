from datetime import datetime, timedelta
from airflow import DAG
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from google_play.operators.google_play_reports import GCSFileTransformOperator
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from google_play.scripts.google_play_utils import SOURCE_BUCKET

DAG_START_DATE = set_env_value(
    production=datetime(2023, 12, 12), dev=datetime(2023, 12, 12)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
GCP_CONN_ID = "sipher_gcp"
DESTINATION_BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
DESTINATION_OBJECT = "google_play"
BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_google_iap"
default_args = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
    "task_concurrency": 3,
    "concurrency": 3,
    "max_active_tasks": 3
}
@task(task_id="create_big_lake_table")
def create_big_lake_table_task(bq_table_name, gcs_prefix, gcs_partition_expr):
    return create_external_bq_table_to_gcs(
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        bq_table=bq_table_name,
        gcs_bucket=DESTINATION_BUCKET,
        gcs_object_prefix=gcs_prefix,
        gcs_partition_expr=gcs_partition_expr,
    )
with DAG(
    dag_id="google_play_iap",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["google_play", "raw_reports"],
    catchup=False
) as dag:
    
    for source in SOURCE_BUCKET:
        with TaskGroup(f"{source}_get_google_play_reports",) as get_google_play_reports:
            financial_reports__earnings_reports = GCSFileTransformOperator(
                task_id= "financial_reports__earnings_reports",
                gcp_conn_id=GCP_CONN_ID,
                source_bucket= SOURCE_BUCKET[source],
                source_object = "earnings",
                destination_bucket= DESTINATION_BUCKET,
                destination_object= f"{DESTINATION_OBJECT}/{source}/financial__earnings_reports",
                ds= "{{ macros.ds_format(macros.ds_add(ds, -30), '%Y-%m-%d', '%Y%m') }}",
            )
            financial_reports__estimated_sales_reports = GCSFileTransformOperator(
                task_id= "financial_reports__estimated_sales_reports",
                gcp_conn_id=GCP_CONN_ID,
                source_bucket= SOURCE_BUCKET[source],
                source_object = "sales",
                destination_bucket= DESTINATION_BUCKET,
                destination_object= f"{DESTINATION_OBJECT}/{source}/financial__estimated_sales_reports",
                ds= "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}",
            )
            statistics_reports__installs_reports = GCSFileTransformOperator(
                task_id= "statistics_reports__installs_reports",
                gcp_conn_id=GCP_CONN_ID,
                source_bucket= SOURCE_BUCKET[source],
                source_object = "stats/installs",
                destination_bucket= DESTINATION_BUCKET,
                destination_object= f"{DESTINATION_OBJECT}/{source}/statistics__installs_reports",
                ds= "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}",
            )
        with TaskGroup(f"{source}_create_big_lake_tables",) as create_big_lake_tables:
            create_big_lake_table_financial_reports__earnings_reports = create_big_lake_table_task.override(
                task_id="create_big_lake_table_financial_reports__earnings_reports"
            )(
                bq_table_name     = f"raw_{source}_financial_reports__earnings_reports",
                gcs_prefix        = f"{DESTINATION_OBJECT}/{source}/financial__earnings_reports",
                gcs_partition_expr= "{snapshot_year_month:STRING}"
            )
            create_big_lake_table_financial_reports__estimated_sales_reports = create_big_lake_table_task.override(
                task_id="create_big_lake_table_financial_reports__estimated_sales_reports"
            )(
                bq_table_name     = f"raw_{source}_financial_reports__estimated_sales_reports",
                gcs_prefix        = f"{DESTINATION_OBJECT}/{source}/financial__estimated_sales_reports",
                gcs_partition_expr= "{snapshot_year_month:STRING}"
            )
            create_big_lake_table_statistics_reports__installs_reports = create_big_lake_table_task.override(
                task_id="create_big_lake_table_statistics_reports__installs_reports"
            )(
                bq_table_name     = f"raw_{source}_statistics_reports__installs_reports",
                gcs_prefix        = f"{DESTINATION_OBJECT}/{source}/statistics__installs_reports",
                gcs_partition_expr= "{snapshot_year_month:STRING}"
            )
        get_google_play_reports >> create_big_lake_tables