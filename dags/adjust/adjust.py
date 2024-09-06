from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow import DAG
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs

from adjust.operators import (
    CopyRawAdjustGcsOperator,
    DeleteRawAdjustGcsOperator
)


DAG_START_DATE = set_env_value(
    production=datetime(2023, 11, 13), dev=datetime(2023, 11, 13)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 */12 * * *", dev="@once")

GCP_CONN_ID = "sipher_gcp"
BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_adjust"




default_args = {
    "owner": "tri.nguyen",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
}




@task(task_id="create_big_lake_table")
def create_big_lake_table_task(bq_table_name, gcs_prefix, gcs_partition_expr):
    return create_external_bq_table_to_gcs(
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        bq_table=bq_table_name,
        gcs_bucket="adjust_raw_event_hiddenatlas_partitioned",
        gcs_object_prefix=gcs_prefix,
        gcs_partition_expr=gcs_partition_expr,
    )




with DAG(
    dag_id="adjust",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["adjust_project", "water_melon", "hidden atlas", "bucket"],
    catchup=False
) as dag:
    

    partition_expr = "{snapshot:DATE}"

    copy_adjust_raw_hidden_atlas_bucket_to_adjust_raw_par_hidden_bucket = CopyRawAdjustGcsOperator(
        task_id='copy_adjust_raw_hidden_atlas_bucket_to_adjust_raw_par_hidden_bucket',
        gcp_conn_id=GCP_CONN_ID,
        init_bucket_name='adjust_raw_event_hiddenatlas',
        des_bucket_name='adjust_raw_event_hiddenatlas_partitioned'
    )
    
    delete_adjust_raw_hidden_atlas_bucket_blobs_after_specific_day = DeleteRawAdjustGcsOperator(
        task_id='delete_adjust_raw_hidden_atlas_bucket_blobs_after_specific_day',
        gcp_conn_id=GCP_CONN_ID,
        bucket_name='adjust_raw_event_hiddenatlas',
        specified_day=10,
        ds="{{ ds }}"
    )

    create_big_lake_table_raw_adjust_hidden_atlas = create_big_lake_table_task.override(task_id="create_big_lake_table_raw_adjust_hidden_atlas")(
        bq_table_name=f"raw_adjust_hidden_atlas",
        gcs_prefix="data",
        gcs_partition_expr=partition_expr,
    )

copy_adjust_raw_hidden_atlas_bucket_to_adjust_raw_par_hidden_bucket >> delete_adjust_raw_hidden_atlas_bucket_blobs_after_specific_day >> create_big_lake_table_raw_adjust_hidden_atlas




