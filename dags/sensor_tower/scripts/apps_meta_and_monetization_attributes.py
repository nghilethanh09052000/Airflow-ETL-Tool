import json
import re
import numpy as np

from datetime import datetime, timedelta
import pandas as pd
import logging
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from utils.data_upload.bigquery_upload import BigQueryDataUpload
from utils.data_extract.http_requests import HTTPRequestsDataExtract

class AppMonMetaAttributes():

    def get_start_date_from_day_diff(self, format='%Y-%m-%d', day_diff=None):
        '''
        To get new date string with different format and day_diff

        Parameters
        ----------
        format : str, default='%Y-%m-%d'
            The date format of date result
        day_diff : int
            The number of day diff needed to parse
        '''
        dd = day_diff if day_diff is None else day_diff
        fdate = datetime.strftime(
            datetime.strptime(self.date, '%Y-%m-%d') - timedelta(days=dd),
            format)
        return fdate

    def __init__(self, ds):
        # self.service_account_json_path = 'airflow-prod-sipher-data-platform-c1999f3f2cc9.json'
        self.service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
        self.bq_dataset_name = 'tower_sensor_data'
        self.project_id = 'data-analytics-342807'

        self.day_diff = 2
        self.date = ds
        self.date_list = [self.get_start_date_from_day_diff(day_diff=i)
                        for i in range(self.day_diff + 1)]
        ds_nodash = self.date.replace('-', '')
        self.bq_table_name = f'app_sales_report_attributes_{ds_nodash}'
        self.uploader = BigQueryDataUpload(service_account_json_path=self.service_account_json_path, dataset_name=self.bq_dataset_name, table_name=self.bq_table_name, project_id=self.project_id)

        self.os = 'unified'
        self.comparison_attribute = 'absolute'
        self.device_type = 'total'
        self.time_range = 'day'
        self.measure = 'revenue'
        self.category_dict = {'7001': 'Games/Action', '7014': 'Games/Role Playing', '7017': 'Games/Strategy'}
        self.auth_token = Variable.get('sensortower_token')
        self.limit = '1000'

        self.path = f'https://api.sensortower.com/v1/{self.os}/sales_report_estimates_comparison_attributes'

    
    @classmethod
    def airflow_callable(cls, ds):
        ins = cls(ds)
        ins.run()


    def run(self):
        for date in self.date_list:
            print(date)
            ds_nodash = date.replace('-', '')
            self.bq_table_name = f'app_meta_and_monetization_attributes{ds_nodash}'
            self.uploader = BigQueryDataUpload(service_account_json_path=self.service_account_json_path, dataset_name=self.bq_dataset_name, table_name=self.bq_table_name, project_id=self.project_id)
            data_df = self.get_data_df(date)
            self.uploader.load_dataframe(data_df)
        

    def get_request(self, path, params):
        extractor = HTTPRequestsDataExtract(url=path, params=params, timeout=100)
        response = extractor.make_request()
        data = json.loads(response.text)
        return data
        
    
    def clean_response_data(self, data_all, category_code):
        logging.info('Start cleaning data')
        data_df = pd.DataFrame(data_all)
        if len(data_all)==0:
            pass
        else:
            data_df_explode = pd.json_normalize(data_df['entities'].explode())

            df_custom_tags = data_df_explode.filter(regex=r"custom_tags.(Monetization|monetization|Meta|meta)")
            for i in df_custom_tags:
                new_value = i.replace(re.search(r"custom_tags.(Meta|meta|Monetization): ", i).group(0), '')
                
                print(new_value)
                print(i)
                df_custom_tags[i] = df_custom_tags[i].str.lower()
                df_custom_tags[i] = np.where(df_custom_tags[i] == "true", new_value, 'nan')
            df_custom_tags['Monetization'] = df_custom_tags.filter(regex=r"Monetization").values.tolist()
            df_custom_tags['Meta'] = df_custom_tags.filter(regex=r"Meta").values.tolist()
            df_custom_tags['Monetization'] = df_custom_tags['Monetization'].apply(lambda row: [val for val in row if val != 'nan'])
            df_custom_tags['Meta'] = df_custom_tags['Meta'].apply(lambda row: [val for val in row if val != 'nan'])
            df_custom_tags = df_custom_tags[['Monetization','Meta']]
            
            data_df_explode['category_code'] = category_code
            data_df_explode['category'] = self.category_dict[category_code]
            data_df_explode = data_df_explode[['app_id','category_code','category']]

            data_df = data_df_explode.merge(df_custom_tags, how = 'inner', left_index=True, right_index=True)
        
        return data_df

    
    def get_data_df(self, date):
        data_df = pd.DataFrame()
        for category in self.category_dict:
            logging.info(f'Getting date from category: {self.category_dict[category]}')
            data = []
            data_all = []
            offset = 0
            while (len(data)==0  and offset==0) or len(data)==int(self.limit):
                params = {'auth_token': self.auth_token
                        ,'device_type': self.device_type
                        ,'comparison_attribute': self.comparison_attribute
                        ,'time_range': self.time_range
                        ,'measure': self.measure
                        ,'category': category
                        ,'date': date
                        ,'limit': self.limit
                        ,'offset': offset
                            }

                data = self.get_request(self.path, params)
                data_all += data
                offset += int(self.limit)
                
                logging.info(f'Number of rows extracted: {len(data_all)}')
            df = self.clean_response_data(data_all, category)
            data_df = pd.concat([data_df, df])
        data_df = data_df.reset_index().drop(columns='index')
        data_df = data_df.astype(str)

        return data_df
