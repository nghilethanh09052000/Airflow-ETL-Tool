from airflow import DAG
from airflow.models import Variable
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from datetime import datetime, timedelta
from fastforex.tasks import (
    create_big_lake_table_task,
    upload_exchange_rate_to_bigquery,
    upload_exchange_rate_to_gcs,
    compare_and_alert_exchange_rates
)


DAG_START_DATE = set_env_value(
    production=datetime(2024, 6, 21), 
    dev=datetime(2024, 6, 19)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@daily")


GCP_CONN_ID = "sipher_gcp"
GCP_PREFIX = 'fast_forex'
BUCKET = 'atherlabs-ingestion'
BIGQUERY_PROJECT = 'sipher-data-platform'
BQ_DATASET = 'fast_forex'




default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_callback,
}

with DAG(
    dag_id="fast_forex",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["Forex", "Exchange Rate", "Message Data"],
    catchup=False
) as dag:
    
    partition_expr = "{snapshot_date:DATE}"
    
    create_big_lake_table_exchange_rate = create_big_lake_table_task.override(task_id="create_big_lake_table_exchange_rate")(
        bq_table_name=f"exchange_rate",
        gcs_prefix=f"{GCP_PREFIX}",
        gcs_partition_expr=partition_expr,
    )
    
    data_upload_exchange_rate_to_gcs = upload_exchange_rate_to_gcs(
        gcs_bucket=BUCKET,
        gcs_prefix=GCP_PREFIX,
        ds='{{ ds }}'
    )
    
    """One Time Run"""
    # data_upload_exchange_rate_to_bigquery = upload_exchange_rate_to_bigquery(
    #     project_id=BIGQUERY_PROJECT,
    #     dataset_id=BQ_DATASET,
    #     table_id=f'base_exchange_rate'
    # )
    
    data_compare_and_alert_exchange_rates = compare_and_alert_exchange_rates(
        project_id=BIGQUERY_PROJECT,
        gcp_conn_id=GCP_CONN_ID,
        ds= '{{ ds }}',
    )
   

    
    data_upload_exchange_rate_to_gcs >> create_big_lake_table_exchange_rate >> data_compare_and_alert_exchange_rates
   