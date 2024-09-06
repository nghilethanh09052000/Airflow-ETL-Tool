
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from google.cloud import storage
from sipher_articles.scripts import (
    get_article_links,
    get_article_infos
)
from datetime import datetime, timedelta


DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 29), 
    dev=datetime(2024, 4, 3)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 */12 * * *", dev="@once")
GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_billing_project")
BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
#BUCKET = 'atherlabs-ingestion'
BQ_DATASET = "sipher_articles"
GCS_PREFIX = "sipher_articles"

@task(task_id='create_big_lake_table_raw_sipher_articles')
def create_big_lake_table_raw_sipher_articles():

    partition_expr = "{snapshot_timestamp:TIMESTAMP}"

    return create_external_bq_table_to_gcs(
        gcp_conn_id        = GCP_CONN_ID,
        bq_project         = BIGQUERY_PROJECT,
        bq_dataset         = BQ_DATASET,
        bq_table           = 'raw_sipher_articles',
        gcs_bucket         = BUCKET,
        gcs_object_prefix  = GCS_PREFIX,
        gcs_partition_expr = partition_expr
    )

@task(task_id='delete_old_snapshots')
def delete_old_snapshots(
    gcs_bucket: str,
    gcs_prefix: str,
    **kwargs
):
    client = storage.Client.from_service_account_json(BaseHook.get_connection(GCP_CONN_ID).extra_dejson[ "key_path"])
    bucket = client.bucket(gcs_bucket)
    blobs = bucket.list_blobs(prefix=gcs_prefix)
    blobs_list = list(blobs)

    blobs_list.sort(key=lambda x: x.name, reverse=True) 

    if len(blobs_list) > 1:  
        blobs_to_delete = blobs_list[1:]  
        for blob in blobs_to_delete:
            blob.delete()

        return f'Deleted {len(blobs_to_delete)} old snapshots from {gcs_bucket}/{gcs_prefix}'
    else:
        return 'No snapshots to delete'


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
    dag_id="sipher_articles",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["Crawl Data", "AI", "Sipher Articles"],
) as dag:



    get_article_links() >> get_article_infos(gcs_bucket=BUCKET, gcs_prefix=GCS_PREFIX) >> create_big_lake_table_raw_sipher_articles() >> delete_old_snapshots(gcs_bucket=BUCKET, gcs_prefix=GCS_PREFIX)
    