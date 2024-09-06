from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from google.cloud import storage
from typing import Dict, List
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from raw_gaming_chain_coinmarketcap_crawl.tasks.get_details_coin.script import GetCoinMarketCapGamingData
import logging
import pandas as pd


class TaskGroupRequiredCoinsDetails(TaskGroup):
     
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
        
        
        """
            Get One Day Required Coins 
            2024-07-04: Tasks to get exchange rate of crypto
        """
        @task(task_group=self)
        def get_list_coins_for_exchage(**kwargs):

            websites = [
                'https://coinmarketcap.com/currencies/tether/',
                'https://coinmarketcap.com/currencies/usd-coin/',
                'https://coinmarketcap.com/currencies/weth/'   ,
                'https://coinmarketcap.com/currencies/usd-coin-bridged-usdc-e/',
                'https://coinmarketcap.com/currencies/sipher/'
                
            ]
            return websites
        
        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_for_exchange_one_day():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = 'raw_coinmarketcap_for_exchange_one_day',
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = "raw_coinmarketcap/for_exchange_one_day",
                gcs_partition_expr = partition_expr,
            )
            
        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_coins_for_exchange_one_day_price(
            website: List[str],
            **kwargs
        ):
            
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website=website,
                day_range='1D' ,
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/for_exchange_one_day"
            ).execute_task()
            
        

        """One Day"""
        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_sipher_one_day():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = 'raw_coinmarketcap_sipher_one_day',
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = "raw_coinmarketcap/sipher_one_day",
                gcs_partition_expr = partition_expr,
            )
        
        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_sipher_coins_one_day_price(**kwargs):
            
            
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website='https://coinmarketcap.com/currencies/sipher/',
                day_range='1D' ,
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/sipher_one_day"
            ).execute_task()
            
        """One Year"""
        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_btc_eth_sipher_one_year():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = 'raw_coinmarketcap_btc_eth_sipher_one_year',
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = "raw_coinmarketcap/btc_eth_sipher",
                gcs_partition_expr = partition_expr,
            )
        
        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_btc_coins_one_year_price(**kwargs):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website='https://coinmarketcap.com/currencies/bitcoin/',
                day_range='1Y',
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/btc_eth_sipher",
            ).execute_task()

        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_eth_coins_one_year_price(**kwargs):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website='https://coinmarketcap.com/currencies/ethereum/',
                day_range='1Y',
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/btc_eth_sipher",
            ).execute_task()

        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_sipher_coins_one_year_price(**kwargs):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website='https://coinmarketcap.com/currencies/sipher/',
                day_range='1Y',
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/btc_eth_sipher",
            ).execute_task()
        
        @task(task_group=self)
        def delete_near_end_blobs_raw_coinmarketcap_one_year():
            """
                Keep the initial ingestion blobs and delete the near end blobs
            """
            client = storage.Client.from_service_account_json(BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"])
            bucket = client.bucket(self.gcs_bucket)
            blobs  = list(client.list_blobs(bucket, prefix="raw_coinmarketcap/btc_eth_sipher"))
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
            get_btc_coins_one_year_price(),
            get_eth_coins_one_year_price(),
            get_sipher_coins_one_year_price()

        ] >> create_big_lake_table_raw_coinmarketcap_btc_eth_sipher_one_year() >> delete_near_end_blobs_raw_coinmarketcap_one_year()


        get_sipher_coins_one_day_price() >> create_big_lake_table_raw_coinmarketcap_sipher_one_day()

        [
             get_coins_for_exchange_one_day_price \
                .partial() \
                .expand(website = get_list_coins_for_exchage())
        ] >> create_big_lake_table_raw_coinmarketcap_for_exchange_one_day()
       
        