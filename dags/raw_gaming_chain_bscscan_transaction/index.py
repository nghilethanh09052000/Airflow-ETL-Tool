from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.decorators import task
from airflow.models import Variable
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from raw_gaming_chain_bscscan_transaction.scripts.index import GetBSCScanTransationsToken

DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 22), 
    dev=datetime(2023, 12, 18)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 0 * * *", dev="0 0 * * *")
GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = 'data-platform-387707'
BQ_DATASET = "raw_gaming_chain"
BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")


GCS_PREFIX = "raw_bscscan_transaction"



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


default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
}

with DAG(
    dag_id="raw_gaming_chain_bscscan_transaction",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["bscscan", "transaction"],
    catchup=False
) as dag:
    
    partition_expr = "{snapshot_date:DATE}"


    create_big_lake_table_raw_bscscan_transaction = create_big_lake_table_task.override(
        task_id="create_big_lake_table_raw_bscscan_transaction"
    )(
        bq_table_name=f"raw_bscscan_transaction",
        gcs_prefix=f"{GCS_PREFIX}",
        gcs_partition_expr=partition_expr
    )
