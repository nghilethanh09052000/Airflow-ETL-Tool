import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import re
import logging
import uuid
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import storage
from airflow.decorators import task
from io import BytesIO

merchant_id = "420246"
format = "csv"

@task(task_id='get_transactions')
def get_transactions(
    gcs_bucket: str, 
    gcs_prefix: str, 
    gcp_conn_id: str,
    ds: str
):
    partition_prefix = f"snapshot_date={ds}"
    url = f"https://api.xsolla.com/merchant/v2/merchants/{merchant_id}/reports/transactions/registry.{format}"

    query = {
        "datetime_from": ds,
        "datetime_to": ds,
        "in_transfer_currency": "1",
        "show_total": "true",
    }

    response = requests.get(url, params=query, auth=('420246', '11f105e13ea07eb93b56c97f5d49dd92fe2c7acf'))

    if response.status_code == 200:
        
        df = pd.read_csv(BytesIO(response.content), dtype={'Transaction ID': str})
        df.columns = [col.lower().replace(' ', '_').replace('.', '_') for col in df.columns]
        print(df)

        client = storage.Client.from_service_account_json(BaseHook.get_connection(gcp_conn_id).extra_dejson["key_path"])
        bucket = client.bucket(gcs_bucket)

        df = df.astype(str)


        table = pa.Table.from_pandas(df)
        parquet_data = BytesIO()
        pq.write_table(table, parquet_data)

        parquet_data.seek(0)

        destination_blob_name = f"{gcs_prefix}/{partition_prefix}/{uuid.uuid4()}.parquet"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(parquet_data, content_type='application/octet-stream')
        logging.info(f"Uploaded report to GCS: {destination_blob_name}")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
