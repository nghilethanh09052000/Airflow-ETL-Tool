from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
from datetime import date, timedelta
import time
import pandas_gbq as gbq
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.hooks.base import BaseHook


class CoinMarketCapETHQuotes5m():

    def __init__(self, **kwargs):
        self.service_account_json_path = BaseHook.get_connection('sipher_gcp').extra_dejson['key_path']
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_json_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.bigquery_client = bigquery.Client(project='sipher-data-platform', credentials = self.credentials)
        self.job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
        self.table_id = 'raw_coinmarketcap.eth_quotes_5m_quote'
        
        self.url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart?'
        self.token_id = '1027'

        self.columns = ['timestamp', 'price_usd', 'market_cap_usd', 'vol_24h', 'price_btc', 'market_cap_btc']
    
    def run(self):
        # self.upload_data_to_bq()
        data_df = self.clean_data_df()
        self.bigquery_client.load_table_from_dataframe(dataframe= data_df ,destination=self.table_id, job_config=self.job_config)
    
    def get_request(self):
        
        parameters = {
            'id': self.token_id,
            'range':'ALL',
        }

        session = Session()
        response = session.get(self.url, params=parameters)
        results = json.loads(response.text)
        data_df =  pd.DataFrame(results['data'])

        return data_df
            
    def clean_data_df(self):
        data_df = self.get_request()

        data_df = data_df['points'].apply(pd.Series)
        data_df = data_df['v'].apply(pd.Series)
        data_df = data_df.reset_index()

        data_df.columns = self.columns        
        
        return data_df