import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.hooks.base import BaseHook


class DailyMainTokenQuotes:
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
        self.job_config.write_disposition = "WRITE_APPEND"

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
        # today_timestamp = int(time.time())
        today_timestamp = (datetime.now(timezone.utc) - timedelta(1)).strftime("%s")
        yesterday_timestamp = (datetime.now(timezone.utc) - timedelta(2)).strftime("%s")

        df_raw = self.get_daily_quote_raw_df(today_timestamp, yesterday_timestamp)
        df_quote = self.get_quote_df(df_raw)

        df_all = df_raw.merge(df_quote, left_index=True, right_index=True)
        df_all = df_all[
            ["id", "name", "symbol", "open", "high", "low", "close", "timestamp"]
        ]

        self.client = bigquery.Client(
            project="sipher-data-platform", credentials=self.credentials
        )
        self.bq_dataset = self.client.dataset(
            "raw_coinmarketcap", project="sipher-data-platform"
        )
        self.bq_table = self.bq_dataset.table("main_token_quotes")
        self.client.load_table_from_dataframe(
            df_all, self.bq_table, job_config=self.job_config
        ).result()

    def get_daily_quote_raw_df(self, today_timestamp, yesterday_timestamp):
        df_raw = pd.DataFrame()
        for token_name in self.token_ids:
            token_id = self.token_ids[token_name]
            url = f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical?id={token_id}&convertId=2781&timeStart={yesterday_timestamp}&timeEnd={today_timestamp}"
            response = requests.get(url)
            response_json = response.json()
            response_df = pd.DataFrame(response_json["data"])
            df_raw = pd.concat([df_raw, response_df])

        df_raw = df_raw.reset_index()
        df_raw = df_raw[["id", "name", "symbol", "quotes"]]
        return df_raw

    def get_quote_df(self, df):
        df_quote = df["quotes"].apply(pd.Series)["quote"].apply(pd.Series)
        df_quote = df_quote[["open", "high", "low", "close", "timestamp"]]
        return df_quote
