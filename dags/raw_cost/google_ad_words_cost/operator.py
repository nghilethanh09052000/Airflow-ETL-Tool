from airflow.hooks.base import BaseHook
from airflow.models import Variable
from typing import Dict, List, Any, Union
from dataclasses import dataclass
from enum import Enum
import logging
import time 
import uuid
from utils.constants import COUNTRY_CODE
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


"""
    Google Ads Words Reporting Data: https://developers.google.com/google-ads/api/docs/reporting/overview
    How to get developer_token, login_customer_id, client_id, client_secret: https://developers.google.com/google-ads/api/docs/get-started/introduction
    How to get refresh_token: https://developers.google.com/google-ads/api/docs/oauth/playground
    Python Lib: https://github.com/googleads/google-ads-python/tree/main
"""

class GoogleAdWordsCostData(GCSDataUpload):

    def __init__(
        self,
        gcs_bucket: str,
        gcs_prefix:str,
        **kwargs
    ):
        
        GCSDataUpload.__init__(
            self, 
            gcs_bucket = gcs_bucket,
            gcs_prefix = gcs_prefix, 
            **kwargs
        )

    def execute(self):
        query = """
            SELECT
                campaign.id,
                ad_group.id,
                ad.id,
                metrics.impressions,
                metrics.clicks
            FROM
                keyword_view
            WHERE
                segments.date DURING LAST_7_DAYS"""


        credentials = self._get_credential()
        client = GoogleAdsClient.load_from_dict(credentials)

        ga_service = client.get_service("GoogleAdsService", version='v15')
        query = """
            SELECT
            campaign.id,
            campaign.advertising_channel_type,
            ad_group.id,
            ad_group.status,
            metrics.impressions,
            metrics.hotel_average_lead_value_micros,
            segments.hotel_check_in_day_of_week,
            segments.hotel_length_of_stay
            FROM hotel_performance_view
            WHERE segments.date DURING LAST_7_DAYS
            AND campaign.advertising_channel_type = 'HOTEL'
            AND ad_group.status = 'ENABLED'
            ORDER BY metrics.impressions DESC
            LIMIT 50
        """
        search_request = client.get_type("SearchGoogleAdsStreamRequest",  version='v15')
        search_request.customer_id = credentials.get('login_customer_id')
        search_request.query = query

        stream = ga_service.search_stream(search_request)

        for batch in stream:
            for row in batch.results:
                campaign = row.campaign
                ad_group = row.ad_group
                hotel_check_in_day_of_week = row.segments.hotel_check_in_day_of_week
                hotel_length_of_stay = row.segments.hotel_length_of_stay
                metrics = row.metrics

                print(
                    f'Ad group ID "{ad_group.id}" '
                    f'in campaign ID "{campaign.id}" '
                )
                print(
                    f'with hotel check-in on "{hotel_check_in_day_of_week}" '
                    f'and "{hotel_length_of_stay}" day(s) stay '
                )
                print(
                    f"had {metrics.impressions:d} impression(s) and "
                    f"{metrics.hotel_average_lead_value_micros:d} average "
                    "lead value (in micros) during the last 7 days.\n"
                )

        
    
    def _get_credential(self) -> Dict[str, str]:

        google_ad_words_token = Variable.get("google_ad_words_token", deserialize_json=True)
        
        credentials = { 
            "developer_token"   : google_ad_words_token.get("google_ad_developer_token"),
            "login_customer_id" : google_ad_words_token.get("google_ad_login_customer_id"),
            "refresh_token"     : google_ad_words_token.get("google_ad_refresh_token"),
            "client_id"         : google_ad_words_token.get("google_ad_client_id"),
            "client_secret"     : google_ad_words_token.get("google_ad_client_secret"),
            "use_proto_plus"    : True
        }
        return credentials

    
    def _get_data(
            self,
        ):
        return

    def _upload_data(self, data):

        collected_ts = round(time.time() * 1000)
        current_date = time.strftime("%Y-%m-%d")
        current_hour = time.strftime("%H")
        partition_prefix = f"snapshot_date={self.params.get('end')}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )




# from exec.cost_class.base_cost import BaseCostClass
# import googleads
# import io
# import pandas as pd
# from exec.utilities import ama_access


# class GoogleadwordsCost(BaseCostClass):

#     def __init__(self, ds, **kwarg):
#         super().__init__(
#             ds=ds,
#             report_is_well_formatted=False
#         )
        
#         self.bq_table_name = 'adwords_cost'
#         self.client_key_name = 'client_id'
#         self.api_key_name = 'client_secret'
#         self.refresh_token_name = 'refresh_token'
#         self.developer_token = ama_access.get_access('googleadwords').get('developer_token')
#         self.columns = ['act_date', 'network_app_id', 'campaign',
#                         'campaign_id', 'location_id', 'impressions', 'clicks',
#                         'installs', 'raw_cost', 'raw_cost_currency']

#         # redefine merge query variables
#         self.platform_query = self.adset_query = self.adset_id_query \
#             = self.ad_creative_query = self.ad_creative_id_query \
#             = self.installsLATOff_query = self.installsLATOn_query = self.ad_type_query = 'NULL'
#         self.raw_cost_currency_query = 'raw_cost_currency'
#         self.media_source_pre  = f'''
#         ,media_source_pre AS
#             (SELECT
#                 * EXCEPT(raw_cost),
#                 CAST(raw_cost AS FLOAT64)/1000000 AS raw_cost
#             FROM `{self.bigquery_project}.{self.bq_dataset_by_day_name}.adwords_cost_*` AS ms
#             LEFT JOIN `amanotes-analytics.performance_monitoring.dim_geotarget` AS cc ON CAST(ms.location_id AS STRING)= cc.Criteria_ID)
#         '''

#     def convert_to_dataframe(self):
#         self.data_df = pd.read_csv(io.StringIO(self.data))

#     def normalize_type(self):
#         if self.data:
#             if self.data_is_grouped_by_day:
#                 self.data_df['act_date'] = pd.to_datetime(self.data_df['act_date']).dt.strftime('%Y-%m-%d')
#             # set location_id type to STRING to prevent error when querying wildcard
#             self.data_df['location_id'] = self.data_df['location_id'].astype('str')

#     def get_account_id_list(self, account):
#         self.get_access_key(account)
#         self.oauth2_client = googleads.oauth2.GoogleRefreshTokenClient(
#             self.client_key,
#             self.api_key,
#             self.refresh_token)
#         # Get account id list
#         client = googleads.adwords.AdWordsClient(
#             self.developer_token, self.oauth2_client,
#             client_customer_id='309-653-5209')  # MCC
#         selector = {'fields': ['CustomerId', 'Name']}
#         accountID = client.GetService('ManagedCustomerService', version='v201809').get(selector)
#         return accountID

#     def get_and_update_data(self, account):
#         accountID = self.get_account_id_list(account)
#         self.result = ''
#         for i in accountID['entries']:
#             try:
#                 client = googleads.adwords.AdWordsClient(
#                     self.developer_token,
#                     self.oauth2_client,
#                     client_customer_id=i['customerId'])
#                 report_downloader = client.GetReportDownloader(version='v201809')
#                 report = {
#                     'reportName': 'report',
#                     'dateRangeType': 'CUSTOM_DATE',
#                     'reportType': 'CAMPAIGN_LOCATION_TARGET_REPORT',
#                     'downloadFormat': 'CSV',
#                     'selector': {
#                         'fields': ['Date',
#                                    'AccountDescriptiveName',
#                                    'CampaignName',
#                                    'CampaignId',
#                                    'Id',
#                                    'Impressions',
#                                    'Clicks',
#                                    'Conversions',
#                                    'Cost',
#                                    'AccountCurrencyCode'],
#                         'dateRange': {'min': self.sd, 'max': self.ed}
#                     }
#                 }
#                 self.result += report_downloader.DownloadReportAsString(
#                     report,
#                     skip_report_header='True',
#                     skip_column_header='True',
#                     skip_report_summary='True',
#                     include_zero_impressions=False
#                 )
#             except Exception:
#                 pass
#         self.data = self.result
#         print('Finished getting and updating data.')