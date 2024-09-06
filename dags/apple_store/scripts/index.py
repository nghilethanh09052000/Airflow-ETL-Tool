import jwt
import requests
import time
import gzip
import logging
import uuid
import re
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import storage
from apple_store.scripts._utils import reports_params
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs

import io
import gzip

def generate_token():
    expiration_in_ten_minutes = int(time.time() + 600)
    payload = {
        "iss": "83c3b166-b13c-40dc-a6e2-edb0e29d3075",  
        "exp": expiration_in_ten_minutes,
        "aud": "appstoreconnect-v1"
    }
    headers = {
        "alg": "ES256",
        "kid": "W3SJ9YK783",  
        "typ": "JWT"
    }
    private_key = Variable.get('apple_store_private_key')
    token = jwt.encode(payload=payload, key=private_key, algorithm="ES256", headers=headers)
    return token


@task(task_id='get_sales_reports')
def get_sales_reports(
    bq_project: str,
    bq_dataset: str,
    gcs_bucket: str, 
    gcs_prefix: str, 
    gcp_conn_id: str,
    ds: str
):
    token = generate_token()
    partition_prefix = f"snapshot_date={ds}"
    
    client = storage.Client.from_service_account_json(BaseHook.get_connection(gcp_conn_id).extra_dejson["key_path"])
    bucket = client.bucket(gcs_bucket)
    
    for params in reports_params:
        url = "https://api.appstoreconnect.apple.com/v1/salesReports"
        try:
            r = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params={
                    "filter[frequency]": params["frequency"], 
                    "filter[reportDate]": ds,
                    "filter[reportSubType]": params["reportSubType"], 
                    "filter[reportType]": params["reportType"],
                    "filter[vendorNumber]": "91583724"
                }
            )
            r.raise_for_status()  

            gzip_content = gzip.decompress(r.content)
        
            df = pd.read_csv(BytesIO(gzip_content), sep='\t')

            df.columns = [re.sub(r'\s+', '_', col.lower()) for col in df.columns]

            df = df.astype(str)

            table = pa.Table.from_pandas(df)
            parquet_data = BytesIO()
            pq.write_table(table, parquet_data)

            parquet_data.seek(0)

            params_prefix = f"{params.get('reportType')}_{params.get('reportSubType')}_{params.get('frequency')}"
            destination_blob_name = f"{gcs_prefix}/{params_prefix}/{partition_prefix}/{uuid.uuid4()}.parquet"
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_file(parquet_data, content_type='application/octet-stream')
            logging.info(f"Uploaded report to GCS: {destination_blob_name}")


            gcs_partition_expr = "{snapshot_date:DATE}"
            create_external_bq_table_to_gcs(
                gcp_conn_id=gcp_conn_id,
                bq_project=bq_project,
                bq_dataset=bq_dataset,
                bq_table=f"raw_sales_reports_{params_prefix}",
                gcs_bucket=gcs_bucket,
                gcs_object_prefix=f"{gcs_prefix}/{params_prefix}",
                gcs_partition_expr=gcs_partition_expr,
            )

        except Exception as e:
            logging.error(f"An error occurred while processing report {params}: {str(e)}")
            continue
 

@task(task_id='get_finance_reports')
def get_finance_reports(
    bq_project: str,
    bq_dataset: str,
    gcs_bucket: str, 
    gcs_prefix: str, 
    gcp_conn_id: str,
    ds: str
):
    token = generate_token()

    region_codes = ["ZZ", "Z1"]
    report_types = ["FINANCIAL", "FINANCE_DETAIL"]
    
    report_month = ds[:7]
    snapshot_year, snapshot_month = report_month.split('-')

    
    client = storage.Client.from_service_account_json(BaseHook.get_connection(gcp_conn_id).extra_dejson["key_path"])
    bucket = client.bucket(gcs_bucket)
    
 
    
    for region_code in region_codes:
        for report_type in report_types:
            
            url = "https://api.appstoreconnect.apple.com/v1/financeReports"
            params={
                "filter[regionCode]": region_code, 
                "filter[reportDate]": report_month,  
                "filter[reportType]": report_type, 
                "filter[vendorNumber]": "91583724"
            }

            try:
                r = requests.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}"
                    },
                    params=params
                )
                with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as f:
                    decompressed_data = f.read() 
                                    
                data_str = decompressed_data.decode('utf-8')
                
                data_str = decompressed_data.decode('utf-8')

                data_lines = data_str.split('\n')

               
                
                data = [line.split('\t') for line in data_lines[1:] if line.strip()]
                max_length = max(len(row) for row in data)
                filtered_data = [row for row in data if len(row) == max_length]
                headers = []
                if region_code == 'ZZ' and report_type == 'FINANCIAL':
                    headers = [
                        "Start Date",
                        "End Date",
                        "UPC",
                        "ISRC/ISBN",
                        "Vendor Identifier",
                        "Quantity",
                        "Partner Share",
                        "Extended Partner Share",
                        "Partner Share Currency",
                        "Sales or Return",
                        "Apple Identifier",
                        "Artist/Show/Developer/Author",
                        "Title",
                        "Label/Studio/Network/Developer/Publisher",
                        "Grid",
                        "Product Type Identifier",
                        "ISAN/Other Identifier",
                        "Country Of Sale",
                        "Pre-order Flag",
                        "Promo Code",
                        "Customer Price",
                        "Customer Currency"
                    ]
                elif region_code == 'Z1' and report_type == 'FINANCE_DETAIL':
                    headers = [
                        "Transaction Date",
                        "Settlement Date",
                        "Apple Identifier",
                        "SKU",
                        "Title",
                        "Developer Name",
                        "Product Type Identifier",
                        "Country of Sale",
                        "Quantity",
                        "Partner Share",
                        "Extended Partner Share",
                        "Partner Share Currency",
                        "Customer Price",
                        "Customer Currency",
                        "Sale or Return",
                        "Promo Code",
                        "Order Type",
                        "Region",
                        "No"
                    ]


                df = pd.DataFrame(filtered_data, columns=headers)
                df.columns = [re.sub(r'\s+', '_', col.lower()) for col in df.columns]

                df = df.astype(str)
                table = pa.Table.from_pandas(df)
                parquet_data = BytesIO()
                pq.write_table(table, parquet_data)

                parquet_data.seek(0)
                
                params_prefix = f"{params.get('filter[reportType]')}"
                destination_blob_name = f"{gcs_prefix}/{params.get('filter[reportType]')}/snapshot_year={snapshot_year}/snapshot_month={snapshot_month}/{uuid.uuid4()}.parquet"
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_file(parquet_data, content_type='application/octet-stream')
                logging.info(f"Uploaded report to GCS: {destination_blob_name}")
                gcs_partition_expr = "{snapshot_year:STRING}/{snapshot_month:STRING}"
                create_external_bq_table_to_gcs(
                    gcp_conn_id=gcp_conn_id,
                    bq_project=bq_project,
                    bq_dataset=bq_dataset,
                    bq_table=f"raw_finance_reports_{params_prefix}",
                    gcs_bucket=gcs_bucket,
                    gcs_object_prefix= f"{gcs_prefix}/{params.get('filter[reportType]')}",
                    gcs_partition_expr=gcs_partition_expr,
                )
                time.sleep(1)
            except Exception as e:
                logging.error(f"An error occurred while processing report: {str(e)}, params: {params}")
                continue
