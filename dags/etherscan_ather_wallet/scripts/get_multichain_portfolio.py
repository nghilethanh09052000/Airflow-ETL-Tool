import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import re
import logging
import uuid
import time
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import storage, bigquery
from airflow.decorators import task
from io import BytesIO
from bs4 import BeautifulSoup
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from lxml import html



@task(task_id='get_multichain_portfolio')
def get_multichain_portfolio(
    gcs_bucket: str, 
    gcs_prefix: str, 
    gcp_conn_id: str,
    bigquery_project: str,
    ds=None,
):
    query = """
        WITH raw_ather_id AS
        (
        SELECT
            DISTINCT
            user_id AS ather_id,
            -- Enabled AS enabled,
            -- UserStatus user_status,
            connected_wallets,
            TRIM(wallets) AS wallets,
            -- email_verified,
            -- name AS user_name,
            -- UserCreateDate AS created_date
        FROM `sipher-data-platform.raw_atherid_gcs.gcs_external_raw_ather_id_user_cognito` 
        ,UNNEST(SPLIT(connected_wallets, ',')) AS wallets
        -- WHERE UserStatus = 'CONFIRMED'
        WHERE
            TRIM(wallets) <> ''
        )

        SELECT
        -- *
        DISTINCT wallets
        FROM raw_ather_id
    """
    service_account_json_path = BaseHook.get_connection(gcp_conn_id).extra_dejson["key_path"]
    bq_client = bigquery.Client.from_service_account_json(service_account_json_path)
    query_job = bq_client.query(query, project=bigquery_project)
    wallets = [row.get('wallets') for row in query_job]
    for wallet in wallets:
        url = f"https://polygonscan.com/address/{wallet}#multichain-portfolio"
        response = requests.get(
            url=url,
            headers={
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0'
            }
        )
        tree = html.fromstring(response.content)
        amount_element = tree.xpath('//a[@id="multichain-button"]/span[@class="fw-medium"]/text()')

        if amount_element:
            amount_text = amount_element[0]
            amount = re.search(r'\$(\d[\d,.]*)', amount_text).group(1).replace(',', '') if amount_text else None
            data = {
                'wallet_address': wallet,
                'multichain_portfolio': amount
            }
            print(data)
            collected_ts = round(time.time() * 1000)
            partition_prefix = f"snapshot_date={ds}"
            gcs_upload = GCSDataUpload(gcs_bucket=gcs_bucket, gcs_prefix=gcs_prefix)
            gcs_upload.upload(
                object_name=f"/{partition_prefix}/{uuid.uuid4()}",
                data=data,
                gcs_file_format=SupportedGcsFileFormat.PARQUET,
                pre_upload_callable=GCSDataUpload._prepare_before_upload(collected_ts)
            )

        else:
            print("Multichain button not found.")