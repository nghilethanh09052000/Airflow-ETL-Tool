from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from typing import Dict
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from raw_gaming_chain_coinmarketcap_crawl.tasks.get_details_coin.script import GetCoinMarketCapGamingData
from datetime import timedelta, datetime
import logging
import pandas as pd


class TaskGroupCoinDetails(TaskGroup):
     
    def __init__(
        self,
        group_id: str,
        bq_project: str,
        bq_dataset: str,
        gcs_bucket: str,
        gcp_conn_id: str,
        tooltip: str = "Get List Coins From Coin MarketCap",
        **kwargs
    ):
        super().__init__(
            group_id=group_id,
            tooltip=tooltip,
            **kwargs
        )
    
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

        @task(task_group=self)
        def get_all_coins_from_xcom(**kwargs):

            ti = kwargs['ti']
            list_coins = ti.xcom_pull(
                key="list_coins",
                task_ids='get_coinmarketcap_list_coins.get_all_coins'
            )
            return list_coins

        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_one_day():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = 'raw_coinmarketcap_one_day',
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = "raw_coinmarketcap/one_day",
                gcs_partition_expr = partition_expr,
            )
        
        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_one_year():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = 'raw_coinmarketcap_one_year',
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = "raw_coinmarketcap/one_year",
                gcs_partition_expr = partition_expr,
            )
        
        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_all_price():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = 'raw_coinmarketcap_all_price',
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = "raw_coinmarketcap/all_time",
                gcs_partition_expr = partition_expr,
            )
        
        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_coins_one_day_price(
            website: Dict[str, str],
            **kwargs
        ):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website=website.get('website'),
                day_range='1D' ,
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/one_day"
            ).execute_task()
        
        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_coins_one_year_price(
            website: Dict[str, str],
            **kwargs
        ):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website=website.get('website'),
                day_range='1Y' ,
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/one_year",
            ).execute_task()
          
        @task(task_group=self)
        def get_coins_all_price(
            website: Dict[str, str],
            **kwargs
        ):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']
       
            return GetCoinMarketCapGamingData(
                website=website.get('website'),
                day_range='ALL' ,
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/all_time",
            ).execute_task()
           
        @task(task_group=self)
        def delete_near_end_blobs_raw_coinmarketcap_one_year():
            """
                Keep the initial ingestion blobs and delete the near end blobs
            """
            client = storage.Client.from_service_account_json(BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"])
            bucket = client.bucket(self.gcs_bucket)
            blobs  = list(client.list_blobs(bucket, prefix="raw_coinmarketcap/one_year"))
            timestamps = [int(blob.name.split("snapshot_timestamp=")[1].split("/")[0]) for blob in blobs]
            df = pd.DataFrame({
                "blobs": blobs,
                "timestamps": timestamps
            })
            grouped_data = df.groupby("timestamps")["blobs"].apply(list).reset_index()
            sorted_data = grouped_data.sort_values(by="timestamps")
           
            
            for index, row in sorted_data.iterrows():
                timestamp = row["timestamps"]
                blobs = row["blobs"]
                if index == 0 or index == len(sorted_data) - 1:
                    logging.info(f"Skipping Deletion For The First Time And Last Timestamp Of Ingestion Data---- {timestamp}")
                else:
                    for blob in blobs:
                        blob.delete()
                        logging.info(f"Blob Delete-------- {blob.name}")
        
        
        [
            get_coins_one_day_price \
                .partial() \
                .expand(website = get_all_coins_from_xcom())
        ] >> create_big_lake_table_raw_coinmarketcap_one_day()


        [
            get_coins_one_year_price \
                .partial() \
                .expand(website = get_all_coins_from_xcom())
        ] >> create_big_lake_table_raw_coinmarketcap_one_year() >> delete_near_end_blobs_raw_coinmarketcap_one_year()


        [
            get_coins_all_price \
                .partial() \
                .expand(website = get_all_coins_from_xcom())
        ] >> create_big_lake_table_raw_coinmarketcap_all_price()
