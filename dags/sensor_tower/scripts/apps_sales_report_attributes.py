import json
from datetime import datetime, timedelta
import pandas as pd
import logging
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from utils.data_upload.bigquery_upload import BigQueryDataUpload
from utils.data_extract.http_requests import HTTPRequestsDataExtract

class AppSalesAttributes():

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
        
        self.columns_filter = ['app_id', 'current_units_value', 'comparison_units_value',
            'units_absolute', 'units_delta', 'units_transformed_delta',
            'current_revenue_value', 'comparison_revenue_value', 'revenue_absolute',
            'revenue_delta', 'revenue_transformed_delta', 'absolute', 'delta', 'transformed_delta', 'date', 'country',
            'custom_tags.Publisher Country', 'custom_tags.Primary Category', 'custom_tags.Game Class', 'custom_tags.Game Genre', 'custom_tags.Game Sub-genre', 'custom_tags.Game Art Style', 'custom_tags.Game Theme', 'custom_tags.Game Setting', 'custom_tags.Release Date (WW)', 'custom_tags.Current US Rating', 'custom_tags.Most Popular Country by Revenue', 'custom_tags.Most Popular Country by Downloads', 'custom_tags.Most Popular Region by Downloads', 'custom_tags.Most Popular Region by Revenue',
            'custom_tags.All Time Publisher Downloads (WW)', 'custom_tags.All Time Publisher Revenue (WW)', 'custom_tags.RPD (All Time, WW)', 'custom_tags.All Time Downloads (WW)','custom_tags.All Time Revenue (WW)', 'custom_tags.Downloads First 30 Days (WW)', 'custom_tags.Revenue First 30 Days (WW)', 'custom_tags.Last 30 Days Downloads (WW)', 'custom_tags.Last 30 Days Revenue (WW)', 'custom_tags.Last 180 Days Downloads (WW)', 'custom_tags.Last 180 Days Revenue (WW)',
            'custom_tags.ARPDAU (Last Month, WW)', 'custom_tags.ARPDAU (Last Month, US)']

        self.columns = ['app_id', 'current_units_value', 'comparison_units_value',
            'units_absolute', 'units_delta', 'units_transformed_delta',
            'current_revenue_value', 'comparison_revenue_value', 'revenue_absolute',
            'revenue_delta', 'revenue_transformed_delta', 'absolute', 'delta', 'transformed_delta', 'date', 'country',
            'publisher_country', 'primary_category', 'game_class', 'game_genre', 'game_sub_genre', 'game_art_style', 'game_theme', 'game_setting', 'release_date_ww', 'current_US_rating', 'most_popular_country_by_revenue', 'most_popular_country_by_downloads', 'most_popular_region_by_revenue', 'most_popular_region_by_downloads',
            'all_time_publisher_downloads_ww', 'all_time_publisher_revenue_ww', 'RPD_all_time_ww', 'all_time_downloads_ww','all_time_revenue_ww', 'downloads_first_30_days_ww', 'revenue_first_30_days_ww', 'last_30_days_downloads_ww', 'last_30_days_revenue_ww', 'last_180_days_downloads_ww', 'last_180_days_revenue_ww',
            'ARPDAU_last_month_ww', 'ARPDAU_last_month_US']

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
            self.bq_table_name = f'app_sales_report_attributes_{ds_nodash}'
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
            data_df = pd.json_normalize(data_df['entities'].explode())
            data_df = data_df.filter(self.columns_filter)
            data_df.columns = self.columns
            data_df['category_code'] = category_code
            data_df['category'] = self.category_dict[category_code]
        
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
