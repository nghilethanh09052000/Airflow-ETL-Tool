from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from open_exchange.operators.latest import OpenExchangeLatestRateOperator
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from utils.dbt import default_args_for_dbt_operators

DAG_START_DATE = set_env_value(
    production=datetime(2023, 5, 9), dev=datetime(2023, 4, 10)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="15 * * * *", dev="@once")

GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_project", "testing-daremi")
BQ_DATASET = "raw_open_exchange"

BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
GCS_PREFIX = "open_exchange"

HTTP_CONN_ID = "open_exchange"

default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
}
default_args.update(default_args_for_dbt_operators)


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


with DAG(
    dag_id="open_exchange_rates",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["open_exchange", "raw"],
    catchup=False,
) as dag:

    gcs_prefix = f"{GCS_PREFIX}/latest"
    bq_table_name = f"open_exchange_latest"
    # dbt_stg_model_name = f"stg_twitter_{report}"
    partition_expr = "{snapshot_date:DATE}/{snapshot_hour:INT64}"

    create_big_lake_table = create_big_lake_table_task(
        bq_table_name=bq_table_name,
        gcs_prefix=gcs_prefix,
        gcs_partition_expr=partition_expr,
    )

    pull_data = OpenExchangeLatestRateOperator(
        task_id=f"get_latest",
        gcs_bucket=BUCKET,
        gcs_prefix=gcs_prefix,
        http_conn_id=HTTP_CONN_ID,
    )

    pull_data >> create_big_lake_table
