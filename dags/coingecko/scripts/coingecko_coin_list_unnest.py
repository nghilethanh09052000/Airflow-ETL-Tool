import json
import requests
import pandas as pd
import pandas_gbq as gbq
from google.oauth2 import service_account
from airflow.models import Variable

from airflow.hooks.base import BaseHook


BIGQUERY_PROJECT = Variable.get("bigquery_project")

class EtherScan():
    '''
        Docstring later
    '''
    def __init__(self, **kwargs):
        super().__init__()
        
        self.bigquery_project = BIGQUERY_PROJECT
        self.path = f'https://api.coingecko.com/api/v3/coins/list'
        self.bq_dataset = 'raw_coingecko'
        self.table_name = 'coins_lists_unnest'
        self.service_account_json_path = BaseHook.get_connection('sipher_gcp').extra_dejson['key_path']

    def run(self):
      self.load_data_event_from_api(include_platform='true')

    def load_data_event_from_api(self, include_platform='true'):
        
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        }
        host = f'{self.path}?include_platform={include_platform}'

        res = requests.get(host, headers=headers)
        data = json.loads(res.text)
        results = []
        for val in data:
            # print(val)
            platforms = val['platforms']
            keys = platforms.keys()
            if len(keys) > 0:
                for x in keys:
                    temp = val
                    temp['platform'] = x
                    temp['address_pl'] = platforms[x]
                    results.append(temp)
                else:
                    results.append(val)
        
        df = pd.DataFrame(results)
        df = df.astype(str)
        self.upload_data(df, insert_type = 'replace')
        
        
    def upload_data(self, df, insert_type = 'replace'):
        table_id = f'{self.bq_dataset}.{self.table_name}'
        project_id = self.bigquery_project
        print(f'Uploading to BigQuery...{project_id}.{table_id}.')
        credentials = service_account.Credentials.from_service_account_file(self.service_account_json_path)
        if df.empty:
            print(': No data')
        else:
            gbq.to_gbq(df, table_id, credentials=credentials, project_id = project_id, if_exists = insert_type) 
            print(f'Uploaded data to BigQuery table {project_id}.{table_id}.')

