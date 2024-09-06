import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import re
import logging
import uuid
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import storage, bigquery
from airflow.decorators import task
from io import BytesIO

merchant_id = "420246"
format = "csv"


@task(task_id='get_transaction_details')
def get_transaction_details(
    gcs_bucket: str, 
    gcs_prefix: str, 
    gcp_conn_id: str,
    bigquery_project: str,
    ds=None
):
  partition_prefix = f"snapshot_date={ds}"
  
  query = """
    SELECT DISTINCT transaction_id FROM `sipher-data-platform.raw_xsolla.raw_xsolla_transactions_reports` 
  """
  
  service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
  bq_client = bigquery.Client.from_service_account_json(service_account_json_path)
  query_job = bq_client.query(query, project=bigquery_project)
  transaction_ids = [row.get('transaction_id') for row in query_job]
  all_data = []
  for transaction_id in transaction_ids:
    url = "https://api.xsolla.com/merchant/v2/merchants/" + merchant_id + "/reports/transactions/" + transaction_id + "/details"

    response = requests.get(url, auth=('420246','11f105e13ea07eb93b56c97f5d49dd92fe2c7acf'))
    
    data = response.json()
    if 'http_status_code' in data: continue

    flat_data = pd.json_normalize(data)


    flat_data.columns = [col.replace('.', '_') for col in flat_data.columns]
    flat_data = flat_data.applymap(str)
    flat_data['transaction_id'] = transaction_id
    all_data.append(flat_data)

  combined_data = pd.concat(all_data, ignore_index=True)

  combined_data = combined_data.astype(str)

  parquet_data = BytesIO()
  table = pa.Table.from_pandas(combined_data)
  pq.write_table(table, parquet_data)

  client = storage.Client.from_service_account_json(BaseHook.get_connection(gcp_conn_id).extra_dejson["key_path"])
  bucket = client.bucket(gcs_bucket)
  parquet_data.seek(0)
  destination_blob_name = f"{gcs_prefix}/{partition_prefix}/{uuid.uuid4()}.parquet"
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_file(parquet_data, content_type='application/octet-stream')
  logging.info(f"Uploaded Parquet file to GCS: {destination_blob_name}")

  return "Data processing and upload completed successfully"
