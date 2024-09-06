from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from google.cloud import bigquery

DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 22), 
    dev=datetime(2024, 3, 7)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="*/60 * * * *", dev="@daily")
GCP_CONN_ID = "sipher_gcp"

default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 8,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_callback,
}

def run_snapshot_table(**kwargs):
    bq_client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
    )
    dataset_id = "sipherg1production.analytics_387396350"
    table_date = kwargs['execution_date'].strftime('%Y%m%d')

    table_name = f'events_intraday_{table_date}'  
    snapshot_table_name = f"snapshot_events_intraday_{table_date}"

    table_id = f"{dataset_id}.{table_name}"
    snapshot_table_id = f"{dataset_id}.{snapshot_table_name}"
    
    try:
        bq_client.get_table(table_id)
        query = f"""
            CREATE OR REPLACE TABLE {snapshot_table_id}
            AS SELECT * FROM {table_id}
        """
        job = bq_client.query(query)
        job.result()  # Wait for the job to complete
        return 'Query executed successfully'
    except Exception as e:
        return f'Error: {e}'


with DAG(
    dag_id="snapshot_sipherg1_production",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["Snapshot Data", "Big Query"],
    catchup=False
) as dag:

    run_snapshot_table = PythonOperator(
        task_id='run_query_task',
        python_callable=run_snapshot_table,
        provide_context=True,
    )
