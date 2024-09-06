import logging
import json
import requests
import pandas as pd
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from utils.data_upload.bigquery_upload import BigQueryDataUpload

class SensortowerTopCreatives():
    
    def __init__(self, ds_nodash):
        self.service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
        self.bq_dataset_name = 'tower_sensor_data'
        self.project_id = 'data-analytics-342807'
        self.bq_table_name = f'top_creatives_{ds_nodash}'
        
        self.os_categories = {
            'android': 'game', 
            'ios': 6014
        }
        self.ad_networks = {
            'Adcolony': 'SDK',
            'Admob': 'SRN', 
            'Applovin': 'SDK', 
            'Chartboost': 'SDK', 
            'Digital Turbine': 'Other', 
            'Facebook': 'SRN', 
            'Facebook Feed': 'SRN', 
            'Instagram': 'SRN', 
            'Mintegral': 'SDK', 
            'Mopub': 'SDK', 
            'Pinterest': 'Other', 
            'Snapchat': 'SRN',
            'Supersonic': 'Publishing',
            'Tiktok': 'SRN', 
            'Twitter': 'SRN', 
            'Unity': 'SDK', 
            'Vungle': 'SDK', 
            'Youtube': 'SRN'
        }
        self.ad_types = ['playable', 'video', 'full_screen', 'banner']
        self.countries = ['US', 'JP', 'CN', 'KR']
    
    @classmethod
    def airflow_callable(cls, ds, ds_nodash):
        ins = cls(ds_nodash)
        ins.run(ds)
        
    def run(self, ds):
        self.df_all = pd.DataFrame()
        for country in self.countries:
            print('Country: ', country)
            for ad_network in self.ad_networks:
                print('Adnetwork: ', ad_network)
                ad_network_type = self.ad_networks[ad_network]
                for ad_type in self.ad_types:
                    print('Type: ', ad_type)
                    for os in self.os_categories:
                        print('OS: ', os)
                        category = self.os_categories[os]
                        self.ad_units_list = self.get_top_app_by_date(ds, os, country, category, ad_network, ad_type)
            
                        self.df_all = pd.concat([self.df_all, self.clean_response_json(ds, country, self.ad_units_list, ad_network_type)])
        
        self.df_all = self.df_all.astype(str)
        self.uploader = BigQueryDataUpload(service_account_json_path=self.service_account_json_path, dataset_name=self.bq_dataset_name, table_name=self.bq_table_name, project_id=self.project_id)
        self.uploader.load_dataframe(self.df_all)
    
    def get_top_app_by_date(self, ds, os, country, category, ad_network, ad_type):
        url_path = f'https://api.sensortower.com/v1/{os}/ad_intel/creatives/top'
        params = {'auth_token': 'ST0_IxfWtBxS_bQy1nsGq7GL2Ee',
                              'date': ds,
                              'country': country,
                              'period': 'week',
                              'category': category,
                              'network': ad_network,
                              'ad_types': ad_type,
                              'limit': 250
                             }

        try:
            response = requests.get(url_path, params=params)
            if response.status_code == 200:
                response_json = json.loads(response.text)
                return response_json['ad_units']
            elif response.status_code == 422:
                print('reponse reason', response.reason)
                return []
            else:
                print('status_code', response.status_code)
                print('reponse reason', response.reason)
        except Exception as e:
            print(e)
    
    def get_list_of_creative_by_app(self, df):    
        rows = []
        for index, row in df['creatives'].items():
            for item in row:
                element = {'index': index, 'creatives': item}
                rows.append(element)
        rows_df = pd.DataFrame(rows)
        creatives_df = pd.concat([rows_df.drop(['creatives'], axis=1), rows_df['creatives'].apply(pd.Series)], axis=1)

        return creatives_df
    
    def clean_response_json(self, ds, country, ad_units_list, ad_network_type):
        ad_units_df = pd.DataFrame(ad_units_list)
        if len(ad_units_df) > 0:
            app_info_df = ad_units_df['app_info'].apply(pd.Series)
            creatives_df = self.get_list_of_creative_by_app(ad_units_df)
            new_ad_units_df = ad_units_df[['network', 'ad_type', 'first_seen_at', 'last_seen_at']]
            app_creative_df = creatives_df.merge(app_info_df, left_on='index', right_index = True, how='left')

            df_all = new_ad_units_df.merge(app_creative_df, left_index=True, right_on='index', how='right')
            df_all['date'] = ds
            df_all['ad_network_channel'] = ad_network_type
            df_all['country'] = country

            return df_all
        else:
            print('response is empty, pass!')
            pass