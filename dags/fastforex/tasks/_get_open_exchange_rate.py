from airflow.models import Variable
from airflow.decorators import task
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from airflow.hooks.base import BaseHook
from google.oauth2 import service_account
from google.cloud import bigquery
import requests
import uuid
import time



GCP_CONN_ID = "sipher_gcp"


def fetch_exchange_rate(api_func):
    def wrapper(*args, **kwargs):
        API_KEY = Variable.get('fast_forex_api_key')
        url = f"https://api.fastforex.io/fetch-all?api_key={API_KEY}"
        
        response = requests.get(url)
        data = response.json()

        exchange_rates = data["results"]
        date = str(data["updated"])
        collected_ts = round(time.time() * 1000)

        api_func(exchange_rates, date, collected_ts, *args, **kwargs)

    return wrapper


@task(task_id='upload_exchange_rate_to_gcs')
@fetch_exchange_rate
def upload_exchange_rate_to_gcs(exchange_rates, date, collected_ts, gcs_bucket, gcs_prefix, ds, **kwargs):
    gcs_upload = GCSDataUpload(
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix
    )
    partition_prefix = f"snapshot_date={str(ds)}"
    
    for currency, value in exchange_rates.items():
        data = {"date": date, "usd": 1, "currency": currency, "value": str(value)}
        gcs_upload.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=gcs_upload._prepare_before_upload(collected_ts=collected_ts)
        )
        
        
@task(task_id='upload_exchange_rate_to_bigquery')
@fetch_exchange_rate
def upload_exchange_rate_to_bigquery(exchange_rates, date, collected_ts ,project_id, dataset_id, table_id, **kwargs):
    credentials = service_account.Credentials.from_service_account_file(
        BaseHook.get_connection(GCP_CONN_ID).extra_dejson["key_path"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
        credentials=credentials,
        project=project_id
    )
    
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    schema = [
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("usd", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("based_value", "STRING"),
        bigquery.SchemaField("value", "STRING"),
        bigquery.SchemaField("exchange_rate", "STRING")
    ]
    
    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
    
    rows_to_insert = [
        {
            "date": str(date),
            "usd": "1",
            "currency": currency,
            "based_value": str(value),
            "value": str(value),
            "exchange_rate": '0'
        }
        for currency, value in exchange_rates.items()
    ]

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    
    return f"Inserted {len(rows_to_insert)} rows into BigQuery table {dataset_id}.{table_id}"
