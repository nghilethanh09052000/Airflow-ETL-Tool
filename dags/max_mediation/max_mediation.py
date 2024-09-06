from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from max_mediation.operators.ad_revenue import MaxAdRevenueOperator
from max_mediation.operators.user_ad_revenue import MaxUserAdRevenueOperator
from max_mediation.operators.cohort import CohortOperator
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from utils.dbt import default_args_for_dbt_operators


DAG_START_DATE = set_env_value(
    production=datetime(2023, 9, 15), dev=datetime(2023, 9, 15)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 4 * * *", dev="@once")

GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_max_mediation"

BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
GCS_PREFIX = "max_mediation"

HTTP_CONN_ID = "max_mediation"

default_args = {
    "owner": "tri.nguyen",
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
    dag_id="max_mediation_reporting",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["max_mediation", "raw"],
    catchup=False
) as dag:

    partition_expr = "{snapshot_date:DATE}"

    create_big_lake_table_ad_revenue = create_big_lake_table_task.override(task_id="create_big_lake_table_ad_revenue")(
        bq_table_name=f"raw_ad_revenue",
        gcs_prefix=f"{GCS_PREFIX}/ad_revenue",
        gcs_partition_expr=partition_expr,
    )

    create_big_lake_table_user_ad_revenue = create_big_lake_table_task.override(task_id="create_big_lake_table_user_ad_revenue")(
        bq_table_name=f"raw_user_ad_revenue",
        gcs_prefix=f"{GCS_PREFIX}/user_ad_revenue",
        gcs_partition_expr=partition_expr,
    )

    create_big_lake_table_cohort_ad_revenue_perpormance = create_big_lake_table_task.override(task_id="create_big_lake_table_cohort_ad_revenue_perpormance")(
        bq_table_name=f"raw_cohort_ad_revenue_perpormance",
        gcs_prefix=f"{GCS_PREFIX}/cohort/ad_revenue_perpormance",
        gcs_partition_expr=partition_expr,
    )

    create_big_lake_table_cohort_ad_impression_information = create_big_lake_table_task.override(task_id="create_big_lake_table_cohort_ad_impression_information")(
        bq_table_name=f"raw_cohort_ad_impression_information",
        gcs_prefix=f"{GCS_PREFIX}/cohort/ad_impression_information",
        gcs_partition_expr=partition_expr,
    )

    create_big_lake_table_cohort_session_information = create_big_lake_table_task.override(task_id="create_big_lake_table_cohort_session_information")(
        bq_table_name=f"raw_cohort_session_information",
        gcs_prefix=f"{GCS_PREFIX}/cohort/session_information",
        gcs_partition_expr=partition_expr,
    )

    pull_ad_revenue_data = MaxAdRevenueOperator(
        task_id=f"get_ad_revenue",
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCS_PREFIX}/ad_revenue",
        http_conn_id=HTTP_CONN_ID,
        ds = "{{ ds }}",
    )

    pull_user_ad_revenue_data = MaxUserAdRevenueOperator(
        task_id=f"get_user_ad_revenue",
        http_conn_id=HTTP_CONN_ID,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCS_PREFIX}/user_ad_revenue",
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        ds = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
    )

    pull_cohort_ad_revenue_perpormance_data = CohortOperator(
        task_id=f"get_cohort_ad_revenue_perpormance",
        http_conn_id=HTTP_CONN_ID,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCS_PREFIX}/cohort/ad_revenue_perpormance",
        ds = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
        endpoint = "/maxCohort"
    )

    pull_cohort_ad_impression_information_data = CohortOperator(
        task_id=f"get_cohort_ad_impression_information",
        http_conn_id=HTTP_CONN_ID,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCS_PREFIX}/cohort/ad_impression_information",
        ds = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
        endpoint = "/maxCohort/imp"
    )

    pull_cohort_session_information_data = CohortOperator(
        task_id=f"get_cohort_session_information",
        http_conn_id=HTTP_CONN_ID,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCS_PREFIX}/cohort/session_information",
        ds = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
        endpoint = f"/maxCohort/session"
    )


    pull_ad_revenue_data >> create_big_lake_table_ad_revenue >> pull_user_ad_revenue_data >> create_big_lake_table_user_ad_revenue

    pull_cohort_ad_revenue_perpormance_data    >> create_big_lake_table_cohort_ad_revenue_perpormance
    pull_cohort_ad_impression_information_data >> create_big_lake_table_cohort_ad_impression_information
    pull_cohort_session_information_data       >> create_big_lake_table_cohort_session_information

    
