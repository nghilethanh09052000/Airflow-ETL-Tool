import requests
import pandas as pd
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.hooks.base import BaseHook


class IntradayMainTokenQuotes:
    def __init__(self):
        self.service_account_json_path = BaseHook.get_connection(
            "sipher_gcp"
        ).extra_dejson["key_path"]
        self.credentials = service_account.Credentials.from_service_account_file(
            self.service_account_json_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.bigquery_project = "sipher-data-platform"
        self.job_config = bigquery.LoadJobConfig()
        self.job_config.write_disposition = "WRITE_TRUNCATE"

        self.token_ids = {
            "BTC": "1",
            "WBTC": "3717",
            "ETH": "1027",
            "stETH": "8085",
            "WETH": "2396",
            "wstETH": "12409",
            "USDT": "825",
            "USDC": "3408",
            "cUSDT": "5745",
            "cUSDC": "5265",
            "Compound": "5692",
        }

    def run(self):
        df_all = self.get_intraday_quote_raw_df()

        self.client = bigquery.Client(
            project="sipher-data-platform", credentials=self.credentials
        )
        self.bq_dataset = self.client.dataset(
            "raw_coinmarketcap", project="sipher-data-platform"
        )
        self.bq_table = self.bq_dataset.table("main_token_quotes_intraday")
        self.client.load_table_from_dataframe(
            df_all, self.bq_table, job_config=self.job_config
        ).result()

    
    def process_response_json(self, response_json, token_symbol):
        data_df = pd.DataFrame(response_json['data'])
        df_unnest = data_df['points'].apply(pd.Series)
        
        df = df_unnest['v'].apply(pd.Series)
        df = df.reset_index()
        df['token_symbol'] = token_symbol
        df.columns = ['timestamp', 'price_usd', 'market_cap_usd', 'vol_24h', 'price_btc', 'market_cap_btc', 'token_symbol']
        # df = df.set_axis(['timestamp', 'price_usd', 'market_cap_usd', 'vol_24h', 'price_btc', 'market_cap_btc', 'token_symbol'], axis=1, inplace=False)
        
        return df


    def get_intraday_quote_raw_df(self):
        df_all = pd.DataFrame()
        try:
            for token_symbol in self.token_ids:
                url = f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart?id={self.token_ids[token_symbol]}&range=1D'
                response = requests.get(url)
                data = json.loads(response.text)
                df_v = self.process_response_json(data, token_symbol)
                df_all = pd.concat([df_all, df_v])
        except Exception as e:
            logging.error(e)

        df_all = df_all.reset_index()    
        df_all = df_all[['timestamp', 'price_usd', 'market_cap_usd', 'vol_24h', 'price_btc', 'market_cap_btc', 'token_symbol']]
        return df_all

    