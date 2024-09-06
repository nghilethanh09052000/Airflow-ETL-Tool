from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql


DAG_START_DATE = set_env_value(
    production=datetime(2024, 8, 14), dev=datetime(2024, 6, 12)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
GCP_CONN_ID = "sipher_gcp"
BUCKET = "ab-testing-crm-data"
BIGQUERY_PROJECT = "sipher-data-platform"
BLOB_NAME = f'gs://{BUCKET}/int_dim_crm_user_info.csv'


import_body = {
    "importContext": {
        "uri": BLOB_NAME,
        "fileType": "CSV",
        "csvImportOptions": {
            "table": "int_dim_crm_user_info",
            "columns": [
                "ather_id",
                "email",
                "ather_user_name",
                "ather_created_timestamp",
                "game_user_id",
                "game_day0_datetime_tzutc",
                "game_user_name"
            ]
        },
        "database": "ab-test",
        "importUser": "data.engineer@atherlabs.com"
    }
}
INSTANCE_NAME = "ab-testing-auth"

default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
    "task_concurrency": 3,
    "concurrency": 3,
    "max_active_tasks": 3
}

@task(task_id = 'truncate_table')
def truncate_table():
    credentials = Variable.get('postgres_cloud_sql_token', deserialize_json=True)
    conn = psycopg2.connect(
        host='34.71.231.45',
        dbname=credentials.get('dbname'),
        user=credentials.get('user'),
        password=credentials.get('password'),
        port=credentials.get('port'),
    )
    conn.autocommit = True
    cursor = conn.cursor()

    truncate_query = sql.SQL("TRUNCATE TABLE {table_name};").format(
        table_name=sql.Identifier('int_dim_crm_user_info')
    )
    cursor.execute(truncate_query)
    print("Table int_dim_crm_user_info truncated successfully.")

with DAG(
    dag_id="biqquery_to_cloudsql",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["BigQuery", "CloudSQL", "Ingestion"],
    catchup=False
) as dag:

    extract_data = BigQueryToGCSOperator(
        task_id='extract_data',
        gcp_conn_id=GCP_CONN_ID,
        source_project_dataset_table=f'sipher-data-platform.sipher_odyssey_core.int_dim_crm_user_info',
        destination_cloud_storage_uris=[BLOB_NAME],
        export_format='CSV',
        dag=dag
    )

    sql_import = CloudSQLImportInstanceOperator(
        instance=INSTANCE_NAME,
        body=import_body,
        project_id='data-platform-387707',
        gcp_conn_id=GCP_CONN_ID,
        task_id='sql_import',
    )

    extract_data >> truncate_table() >> sql_import
