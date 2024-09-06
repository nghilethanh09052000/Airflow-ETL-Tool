from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from raw_gaming_chain_coinmarketcap_crawl.tasks import (
    TaskGroupGetListCoins,
    TaskGroupCoinDetails,
    TaskGroupRequiredCoinsDetails
) 

DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 22), 
    dev=datetime(2024, 1, 11)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 */2 * * *", dev="0 */2 * * *")
GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_billing_project")
#BIGQUERY_PROJECT = Variable.get("bigquery_project")
BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
#BUCKET = "atherlabs-ingestion"

BQ_DATASET = "raw_gaming_chain"
GCS_PREFIX = "raw_coinmarketcap"


default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 8,
    "retry_delay": timedelta(minutes=4),
    "on_failure_callback": airflow_callback,
}

with DAG(
    dag_id="raw_gaming_chain_coinmarketcap_crawl",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["coinmarketcap", "crawl_data", "game_data"],
    catchup=False
) as dag:
    
    
    # get_coinmarketcap_list_coins = TaskGroupGetListCoins(
    #     group_id='get_coinmarketcap_list_coins',
    #     bq_project=BIGQUERY_PROJECT, 
    #     bq_dataset=BQ_DATASET,
    #     gcs_bucket=BUCKET,
    #     gcs_prefix=f'{GCS_PREFIX}/list_coins',
    #     gcp_conn_id=GCP_CONN_ID
    # )

    # get_coinmarketcap_coin_details = TaskGroupCoinDetails(
    #     group_id='get_coinmarketcap_coin_details',
    #     bq_project=BIGQUERY_PROJECT,
    #     bq_dataset=BQ_DATASET,
    #     gcs_bucket=BUCKET,
    #     gcp_conn_id=GCP_CONN_ID
    # )

    get_coinmarketcap_required_coin = TaskGroupRequiredCoinsDetails(
        group_id='get_coinmarketcap_required_coin',
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        gcs_bucket=BUCKET,
        gcp_conn_id=GCP_CONN_ID
    )

    

    #get_coinmarketcap_list_coins >> get_coinmarketcap_coin_details

    get_coinmarketcap_required_coin




