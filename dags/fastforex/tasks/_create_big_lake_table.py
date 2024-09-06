from airflow.decorators import task
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs


GCP_CONN_ID = "sipher_gcp"
GCP_PREFIX = 'fast_forex'
BUCKET = 'atherlabs-ingestion'
BIGQUERY_PROJECT = 'sipher-data-platform'
BQ_DATASET = 'fast_forex'

@task(task_id="create_big_lake_table")
def create_big_lake_table_task(bq_table_name, gcs_prefix, gcs_partition_expr):
    return create_external_bq_table_to_gcs(
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        bq_table=bq_table_name,
        gcs_bucket=BUCKET,
        gcs_object_prefix=gcs_prefix,
        gcs_partition_expr=gcs_partition_expr,
)
   