from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
import pandas_gbq as gbq
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.hooks.base import BaseHook
from airflow.models import Variable


class CoinMarketCapCurrency():

    def __init__(self, **kwargs):
        self.service_account_json_path = BaseHook.get_connection('sipher_gcp').extra_dejson['key_path']
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_json_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.client = bigquery.Client(project='sipher-data-platform', credentials = self.credentials)
        self.bigquery_project = 'sipher-data-platform'
        self.bq_dataset = 'raw_coinmarketcap'
        self.table_id = 'currency_latest_listings'
        
        self.url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        self.authenciation_token = Variable.get('pro_coinmarketcap_token')
        self.headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': f'{self.authenciation_token}',
        }
        self.columns = ['id', 'name', 'symbol', 'slug', 'num_market_pairs', 'date_added',
                        'tags', 'max_supply', 'circulating_supply', 'total_supply', 'platform',
                        'cmc_rank', 'self_reported_circulating_supply',
                        'self_reported_market_cap', 'last_updated', 'price',
                        'volume_24h', 'volume_change_24h',
                        'percent_change_1h', 'percent_change_24h',
                        'percent_change_7d', 'percent_change_30d',
                        'percent_change_60d', 'percent_change_90d',
                        'market_cap', 'market_cap_dominance',
                        'fully_diluted_market_cap', 'quote_last_updated',
                        'platform_id', 'platform_name', 'platform_symbol', 'platform_slug',
                        'platform_token_address']
    
    def run(self):
        self.upload_data_to_bq()
    
    def get_request(self):
        start = 1
        listings_number = 20000
        step = 1000
        data_df = pd.DataFrame()

        for start in range(start, listings_number, step):
            
            parameters = {
            'start':f'{start}',
            'limit':f'{step}',
            'convert':'USD'
            }

            session = Session()
            session.headers.update(self.headers)

            try:
                response = session.get(self.url, params=parameters)
                results = json.loads(response.text)
                if results['status']['credit_count'] != 0:
                    results_df =  pd.DataFrame(results['data'])
                    data_df= pd.concat([data_df, results_df], ignore_index=True)
                else:
                    break
            except (ConnectionError, Timeout, TooManyRedirects) as e:
                print(e)
        return data_df
            
    def clean_data_df(self):
        data_df = self.get_request()

        json_struct = json.loads(data_df.to_json(orient="records"))    
        data_df_flat = pd.json_normalize(json_struct)

        data_df_flat = data_df_flat.filter(['id', 'name', 'symbol', 'slug', 'num_market_pairs', 'date_added',
                        'tags', 'max_supply', 'circulating_supply', 'total_supply', 'platform',
                        'cmc_rank', 'self_reported_circulating_supply',
                        'self_reported_market_cap', 'last_updated', 'quote.USD.price',
                        'quote.USD.volume_24h', 'quote.USD.volume_change_24h',
                        'quote.USD.percent_change_1h', 'quote.USD.percent_change_24h',
                        'quote.USD.percent_change_7d', 'quote.USD.percent_change_30d',
                        'quote.USD.percent_change_60d', 'quote.USD.percent_change_90d',
                        'quote.USD.market_cap', 'quote.USD.market_cap_dominance',
                        'quote.USD.fully_diluted_market_cap', 'quote.USD.last_updated',
                        'platform.id', 'platform.name', 'platform.symbol', 'platform.slug',
                        'platform.token_address'])
        
        data_df_flat.columns = self.columns
        data_df_flat = data_df_flat.astype({'tags': str, 'fully_diluted_market_cap': float, 'total_supply': float})        
        
        return data_df_flat
    
    def upload_data_to_bq(self):
        data =  self.clean_data_df()

        print('Uploading to BigQuery...')
        table_id = f'{self.bq_dataset}.{self.table_id}'
        project_id = self.bigquery_project
        credentials = self.credentials
        # table_schema = pandas_gbq.schema.update_schema(table_schema, original_schema)
        if data.empty:
            print(': No data')
        else:
            data.to_gbq(table_id, credentials=credentials, project_id = project_id, if_exists = 'append') 
            # replace | append
            print(f'Uploaded data to BigQuery table {project_id}.{table_id}.')   
