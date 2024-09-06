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


class CoinMarketCapETHQuotes():

    def __init__(self, **kwargs):
        self.service_account_json_path = BaseHook.get_connection('sipher_gcp').extra_dejson['key_path']
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_json_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.bigquery_client = bigquery.Client(project='sipher-data-platform', credentials = self.credentials)
        self.job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
        self.table_id = 'raw_coinmarketcap.eth_quotes'
        
        self.url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical'

        self.columns = ['timeOpen', 'timeClose', 'timeHigh', 'timeLow', 'quote_open', 'quote_high', 'quote_low', 'quote_close', 'quote_volume', 'quote_marketCap', 'quote_timestamp']
    
    def run(self):
        # self.upload_data_to_bq()
        data_df = self.clean_data_df()
        self.bigquery_client.load_table_from_dataframe(dataframe= data_df ,destination=self.table_id, job_config=self.job_config)
    
    def get_request(self):
        today = date.today()
        week_ago = today - timedelta(days=3)
        start_timestamp_float = time.mktime(week_ago.timetuple())
        start_timestamp_int = int(start_timestamp_float)
        end_timestamp_float = time.mktime(today.timetuple())
        end_timestamp_int = int(end_timestamp_float)

        data_df = pd.DataFrame()

        parameters = {
                    'id':'1027',
                    'convertId':'2781',
                    'timeStart':start_timestamp_int,
                    'timeEnd':end_timestamp_int
                    }

        session = Session()

        try:
            response = session.get(self.url, params=parameters)
            results = json.loads(response.text)
            if results['status']['error_code'] == '0':
                results_df =  pd.DataFrame(results['data']['quotes'])
                data_df= pd.concat([data_df, results_df], ignore_index=True)
            else:
                print('Error in data response')
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
        return data_df
            
    def clean_data_df(self):
        data_df = self.get_request()

        json_struct = json.loads(data_df.to_json(orient="records"))    
        data_df_flat = pd.json_normalize(json_struct)

        data_df_flat = data_df_flat.filter(['timeOpen', 'timeClose', 'timeHigh', 'timeLow', 'quote.open', 'quote.high', 'quote.low', 'quote.close', 'quote.volume', 'quote.marketCap', 'quote.timestamp'])
        
        data_df_flat.columns = self.columns
        
        return data_df_flat