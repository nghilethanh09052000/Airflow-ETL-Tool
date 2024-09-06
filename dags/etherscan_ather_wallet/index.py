from datetime import datetime, timedelta
from airflow import DAG
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from airflow.models import Variable
from airflow.decorators import task
from etherscan_ather_wallet.scripts import (
    get_multichain_portfolio
)
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 22), dev=datetime(2024, 4, 3)
)

DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
GCP_CONN_ID = "sipher_gcp"
DESTINATION_BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
DESTINATION_OBJECT = "etherscan_ather_wallet"
BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_multichain"



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
    dag_id="etherscan_ather_wallet",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["etherscan", "crawl_data", "ather_wallets"],
    catchup=False
) as dag:
    
    partition_expr = "{snapshot_date:DATE}"

    get_multichain_portfolio(
        gcs_bucket=DESTINATION_BUCKET, 
        gcs_prefix=f"{DESTINATION_OBJECT}/multichain_portfoilio", 
        gcp_conn_id=GCP_CONN_ID,
        bigquery_project=BIGQUERY_PROJECT,
        ds="{{ds}}"
    )

    create_big_lake_table_etherscan_ather_wallet_multichain_portfoilio = create_big_lake_table_task.override(task_id="create_big_lake_table_etherscan_ather_wallet_multichain_portfoilio")(
        bq_table_name=f"ather_wallets_multichain_portfolio",
        gcs_prefix=f"{DESTINATION_OBJECT}/multichain_portfoilio",
        gcs_partition_expr=partition_expr,
    )

    webshop_eth_tx = BigQueryInsertJobOperator(
        task_id="webshop_eth_tx",
        configuration={
            "query": {
                "query": "{% include 'query/webshop_eth_tx.sql' %}",  
                "useLegacySql": False
            }
        },
        gcp_conn_id="sipher_gcp",
        dag=dag,
    )

    webshop_polygon_tx = BigQueryInsertJobOperator(
        task_id="webshop_polygon_tx",
        configuration={
            "query": {
                "query": "{% include 'query/webshop_polygon_tx.sql' %}",  
                "useLegacySql": False
            }
        },
        gcp_conn_id="sipher_gcp",
        dag=dag,
    )