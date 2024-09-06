import json
import requests
import pandas as pd
import numpy as np
import logging
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from google.cloud import bigquery
from utils.data_upload.bigquery_upload import BigQueryDataUpload

BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

class AppsMetadata():

    def __init__(self):

        self.service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
        self.bq_dataset_name = 'raw_sensortower'
        self.project_id = BIGQUERY_PROJECT
        self.bq_table_name = 'app_metadata'

        self.query = """
            SELECT DISTINCT app_id
            FROM `sipher-data-platform.raw_sensortower.raw_store_intelligence_sales_report_estimates_comparison_attributes`
            WHERE game_class = 'true'
            UNION ALL
            SELECT DISTINCT app_id
            FROM `sipher-data-platform.raw_sensortower.daily_top_apps_*`
        """
        self.auth_token = Variable.get('sensortower_token')

        self.columns = ['app_id', 'name', 'humanized_name', 'publisher_id', 'publisher_name', 'os']


    def run(self):

        app_id_df = self.get_current_app_id()
       
        ios_app_list = app_id_df.loc[app_id_df['os'] == 'ios']['app_id'].tolist()
        android_app_list = app_id_df.loc[app_id_df['os'] == 'android']['app_id'].tolist()
        
        self.apps_data_list = []
        self.get_new_app_id('ios', ios_app_list)
        self.get_new_app_id('android', android_app_list)

        self.apps_data_df = pd.DataFrame(self.apps_data_list)
        self.apps_data_df = self.apps_data_df[self.columns]
        self.apps_data_df = self.apps_data_df.astype(str)

        self.uploader = BigQueryDataUpload(
            service_account_json_path=self.service_account_json_path, 
            dataset_name=self.bq_dataset_name, 
            table_name=self.bq_table_name, 
            project_id=self.project_id
        )
        self.uploader.load_dataframe(self.apps_data_df)
    

    @classmethod
    def airflow_callable(cls):
        ins = cls()
        ins.run()


    def get_current_app_id(self):
        logging.info('Getting current apps id list')
        bq_client = bigquery.Client.from_service_account_json(self.service_account_json_path)
        query_job = bq_client.query(self.query, project=BIGQUERY_BILLING_PROJECT)

        app_id = []
        for row in query_job:
            app_id.append(row['app_id'])
        
        df = pd.DataFrame({'app_id':app_id})
        df['os'] = np.where(df['app_id'].str.isnumeric(), 'ios', 'android')

        return df

    
    def get_new_app_id(self, app_os, app_list):
        logging.info('Getting apps metadata')
        apps_path = f'https://api.sensortower.com/v1/{app_os}/apps'
        limit = 100
        offset = 0
        while limit <= len(app_list):
            app_list_str = ','.join(app_list[offset:limit])
            apps_params = {'auth_token': self.auth_token
                    ,'app_ids': app_list_str
                    }

            apps_res = requests.get(apps_path, params=apps_params)
            apps_data = json.loads(apps_res.text)
            self.apps_data_list += apps_data['apps']
            offset = limit
            limit += 100
            print(limit)

