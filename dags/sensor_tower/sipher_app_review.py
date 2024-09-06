from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from sensor_tower.tasks.get_reviews.task_sipher import (
    task_sipher_get_review_us_country_on_specific_app_ids
)

DAG_START_DATE = set_env_value(production=datetime(
    2024, 3, 8), dev=datetime(2024, 3, 8))
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 */12 * * *", dev="@daily")


BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_sensortower"

BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
GCS_PREFIX = "sipher_app_review"
GCP_CONN_ID = "sipher_gcp"


HTTP_CONN_ID = "sensortower"

default_args = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "trigger_rule": "all_done",
    "retries": 2,
    "retry_delay":  timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
    "task_concurrency": 3,
    "concurrency": 3,
}


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
    dag_id="sipher_app_review",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["sensortower", "sipher"],
    catchup=False,
) as dag:

    partition_expr = "{snapshot_date:DATE}"

    """Refactor Based On Max Mediation"""

    create_big_lake_table_get_sipher_reviews_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_get_sipher_reviews_data"
    )(
        bq_table_name=f"raw_sipher_app_review",
        gcs_prefix=f"{GCS_PREFIX}/app_intelligence/reviews/get_reviews",
        gcs_partition_expr=partition_expr
    )

    with TaskGroup(
        "get_sipher_reviews_data",
        tooltip='Get App Intelligence: Get Reviews Data: https://app.sensortower.com/api/docs/app_intel#/Reviews/get_reviews and Upload It To Big Query In "raw_app_intelligence_get_reviews" Table'
    ) as get_sipher_reviews_data:
        
        task_sipher_get_review_us_country_on_specific_app_ids(
            ds="{{ ds }}",
            task_id='task_sipher_get_review_us_country_on_specific_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )

        [
            task_sipher_get_review_us_country_on_specific_app_ids
        ]

get_sipher_reviews_data >> create_big_lake_table_get_sipher_reviews_data

