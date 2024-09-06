import requests
from datetime import datetime
import time
import json
import pandas as pd
import pandas_gbq as gbq
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.hooks.base import BaseHook
from airflow.models import Variable

BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

class EtherScanCurrency():

    def __init__(self, **kwargs):
        self.service_account_json_path = BaseHook.get_connection('sipher_gcp').extra_dejson['key_path']
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_json_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.client = bigquery.Client(project=BIGQUERY_PROJECT, credentials = self.credentials)
        self.bigquery_project = BIGQUERY_PROJECT
        self.bq_dataset = 'raw_coingecko'
        self.table_name = 'coins_currency'
        self.path = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids='
        # self.schema = [{'name':'id', 'type': 'STRING'},
        #                 {'name': 'symbol', 'type': 'STRING'},
        #                 {'name': 'name', 'type': 'STRING'},
        #                 {'name': 'image', 'type': 'STRING'},
        #                 {'name': 'current_price', 'type': 'FLOAT'},
        #                 {'name': 'market_cap', 'type': 'FLOAT'},
        #                 {'name': 'market_cap_rank', 'type': 'FLOAT'},
        #                 {'name': 'fully_diluted_valuation', 'type': 'FLOAT'},
        #                 {'name': 'total_volume', 'type': 'FLOAT'},
        #                 {'name': 'high_24h', 'type': 'FLOAT'},
        #                 {'name': 'low_24h', 'type': 'FLOAT'},
        #                 {'name': 'price_change_24h', 'type': 'FLOAT'},
        #                 {'name': 'price_change_percentage_24h', 'type': 'FLOAT'},
        #                 {'name': 'market_cap_change_24h', 'type': 'FLOAT'},
        #                 {'name': 'market_cap_change_percentage_24h', 'type': 'FLOAT'},
        #                 {'name': 'circulating_supply', 'type': 'FLOAT'},
        #                 {'name': 'total_supply', 'type': 'FLOAT'},
        #                 {'name': 'max_supply', 'type': 'FLOAT'},
        #                 {'name': 'ath', 'type': 'FLOAT'},
        #                 {'name': 'ath_change_percentage', 'type': 'FLOAT'},
        #                 {'name': 'ath_date', 'type': 'STRING'},
        #                 {'name': 'alt', 'type': 'FLOAT'},
        #                 {'name': 'atl_change_percentage', 'type': 'FLOAT'},
        #                 {'name': 'atl_date', 'type': 'STRING'},
        #                 {'name': 'roi', 'type': 'STRING'},
        #                 {'name': 'last_updated', 'type': 'STRING'},
        #                 {'name': 'upload_time', 'type': 'STRING'},
        #                ]
        self.schema = [{'name':'id', 'type': 'STRING'},
                        {'name': 'symbol', 'type': 'STRING'},
                        {'name': 'name', 'type': 'STRING'},
                        {'name': 'image', 'type': 'STRING'},
                        {'name': 'current_price', 'type': 'STRING'},
                        {'name': 'market_cap', 'type': 'STRING'},
                        {'name': 'market_cap_rank', 'type': 'STRING'},
                        {'name': 'fully_diluted_valuation', 'type': 'STRING'},
                        {'name': 'total_volume', 'type': 'STRING'},
                        {'name': 'high_24h', 'type': 'STRING'},
                        {'name': 'low_24h', 'type': 'STRING'},
                        {'name': 'price_change_24h', 'type': 'STRING'},
                        {'name': 'price_change_percentage_24h', 'type': 'STRING'},
                        {'name': 'market_cap_change_24h', 'type': 'STRING'},
                        {'name': 'market_cap_change_percentage_24h', 'type': 'STRING'},
                        {'name': 'circulating_supply', 'type': 'STRING'},
                        {'name': 'total_supply', 'type': 'STRING'},
                        {'name': 'max_supply', 'type': 'STRING'},
                        {'name': 'ath', 'type': 'STRING'},
                        {'name': 'ath_change_percentage', 'type': 'STRING'},
                        {'name': 'ath_date', 'type': 'STRING'},
                        {'name': 'alt', 'type': 'STRING'},
                        {'name': 'atl_change_percentage', 'type': 'STRING'},
                        {'name': 'atl_date', 'type': 'STRING'},
                        {'name': 'roi', 'type': 'STRING'},
                        {'name': 'last_updated', 'type': 'STRING'},
                        {'name': 'upload_time', 'type': 'STRING'},
                       ]
    
    def run(self):
        results_array = self.iter_data()
        for indx,val in enumerate(results_array):
            print(indx)
            self.load_data_event_from_api(index=indx, array_ids=val)
            time.sleep(2)

    def upload_data_to_bq(self, df, insert_type = 'replace'):
        print('Uploading to BigQuery...')
        table_id = f'{self.bq_dataset}.{self.table_name}'
        project_id = self.bigquery_project
        credentials = self.credentials
        if df.empty:
            print(': No data')
        else:
            gbq.to_gbq(df, table_id, credentials=credentials, project_id = project_id, if_exists = insert_type, table_schema = self.schema) 
            # replace | append
            print(f'Uploaded data to BigQuery table {project_id}.{table_id}.')    
        
    def load_data_event_from_api(self, index=0, array_ids=['']):
        
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        }
        to_str = ','.join(str(item) for item in array_ids)
        host = f'{self.path}{to_str}'

        res = requests.get(host, headers=headers)
        if res.status_code == 200:
            data = json.loads(res.text)

            df = pd.DataFrame(data)
            df['upload_time'] = datetime.now()
            df = df.astype(str)
            if index == 0:
                self.upload_data_to_bq(df, insert_type = 'replace')
            else:
                self.upload_data_to_bq(df, insert_type = 'append')
        else:
            print('status_code', res.status_code)
            print('reponse reason', res.reason)
            
    def iter_data(self):
        # Create and Storage at BiQuery
        # results = client.query(query_base) # Using to Create table on BigQuery

        # get data from extraction layer - get data from Biquery when you stored data at the line above
        query_data_extraction = '''
           SELECT DISTINCT id FROM `sipher-data-platform.raw_coingecko.coins_lists_unnest` 
          WHERE platform = 'ethereum'
        '''
        df = self.client.query(query_data_extraction, project=BIGQUERY_BILLING_PROJECT).to_dataframe()
            
        index_circle = 0
        len_x = 200
        data_ids = []
        temp = []
        for index, row in df.iterrows():
          # print(index)
            data_ids.append(row['id'])
            temp.append(row['id'])
        results_array = []
        while len(data_ids):
            results_array.append(data_ids[0:len_x])
            del data_ids[0:len_x]
        return results_array