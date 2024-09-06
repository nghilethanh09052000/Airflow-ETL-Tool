from airflow.hooks.base import BaseHook
from airflow.models import Variable
from typing import Dict, List, Any, Union
from dataclasses import dataclass
import time 
import uuid
from utils.constants import COUNTRY_CODE
from enum import Enum
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
import requests
import logging
from .utils import (
    Scale,
    SplitBy,
    Fields,
    AdTypes,
    AppStores,
    Platforms
)


"""
https://services.docs.unity.com/statistics/v1/index.html#tag/Acquisitions/operation/stats_acquisition
"""

@dataclass 
class UnityAdCostParams:
    start                : str
    end                  : str
    scale                : str
    splitBy              : str
    fields               : str
    campaignSets         : str
    campaigns            : str
    targets              : str
    adTypes              : str
    countries            : str
    stores               : str
    platforms            : str
    osVersions           : str
    creativePacks        : str
    sourceAppIds         : str
    skadConversionValues : str

    def _handle_check_enum_string(
            self,
            string_comma: str,
            EnumClass: Enum
        ) -> bool:     

        if ',' not in string_comma: 
            return string_comma in [enum_item.value for enum_item in EnumClass]
        
        return all(
            item in [
                enum_item.value 
                    for enum_item in EnumClass
            ] for item in string_comma.split(',')
        ) 

    def __post_init__(self):
        
        if not (self.start or self.end):
            raise ValueError('Start Date and End Data Must Be Specified')
        
        if self.scale and not self._handle_check_enum_string(self.scale, Scale):
            raise ValueError('Scale Params Must Be In The List', Scale)
        
        if self.splitBy and not self._handle_check_enum_string(self.splitBy, SplitBy):
            raise ValueError("SplitBy Params Must Be In The List", SplitBy)
        
        if self.fields and not self._handle_check_enum_string(self.fields, Fields):
            raise ValueError("Fields Params Must Be In The List", Fields)
        
        if self.adTypes and not self._handle_check_enum_string(self.adTypes, AdTypes):
            raise ValueError("Ad Types Params Must Be In The List", AdTypes)
        
        if self.countries and not [item in list(COUNTRY_CODE.keys()) for item in self.countries.split(',')] :
             raise ValueError("Countries Must Be In The List", COUNTRY_CODE)
        
        if self.stores and not self._handle_check_enum_string(self.stores, AppStores):
            raise ValueError("App Stores Params Must Be In The List", AppStores)
        
        if self.platforms and not self._handle_check_enum_string(self.platforms, Platforms):
            raise ValueError("Platforms Params Must Be In The List", Platforms)
        
@dataclass
class UnityAdCostDataResults:
    timestamp       : str
    campaignSet     : str
    adType          : str
    spend           : str
    clicks          : str
    store           : str
    starts          : str
    views           : str
    installs        : str
    campaignSetName : str
    ctr             : str
    cpi             : str
    cvr             : str
    ecpm            : str


class UnityAdCostData(GCSDataUpload):

    def __init__(
        self,
        gcs_bucket: str,
        gcs_prefix:str,
        params,
        **kwargs
    ):
        
        GCSDataUpload.__init__(
            self, 
            gcs_bucket = gcs_bucket,
            gcs_prefix = gcs_prefix, 
            **kwargs
        )

        self.params = params

    def execute(self):

        results = self._get_data()


        if not results:
            logging.info(f'There Is No Data To Upload, Skip The Task In Current Day: {self.params.get("end")}')
            return
        data = self._format_response_result(results=results)
        self._upload_data(data=data)

    def _get_data(self):

        http_hook                = BaseHook.get_connection('unity_cost')
        unity_organization_token = Variable.get('unity_token', deserialize_json=True).get('unity_organization_token')
        unity_api_token          = Variable.get('unity_token', deserialize_json=True).get('unity_api_token')
        api_url                  = f"{http_hook.host}organizations/{unity_organization_token}/reports/acquisitions"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {unity_api_token}"
        }

        try:
            response = requests.get(
                    api_url, 
                    params=self.params, 
                    headers=headers
            )
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Error in API request: {e}")

    def _format_response_result(
            self,
            results: List[Union[str, Dict[str, Any]]]
        )->List[UnityAdCostDataResults]:

        return [

            UnityAdCostDataResults(
                timestamp       = str(result.get('timestamp')),
                campaignSet     = str(result.get('result', '').get('campaignSet', '')),
                adType          = str(result.get('result', '').get('adType', '')),
                spend           = str(result.get('result', '').get('spend', '')),
                clicks          = str(result.get('result', '').get('clicks', '')),
                store           = str(result.get('result', '').get('store', '')),
                starts          = str(result.get('result', '').get('starts', '')),
                views           = str(result.get('result', '').get('views', '')),
                installs        = str(result.get('result', '').get('installs', '')),
                campaignSetName = str(result.get('result', '').get('campaignSetName', '')),
                ctr             = str(result.get('result', '').get('ctr', '')),
                cpi             = str(result.get('result', '').get('cpi', '')),
                cvr             = str(result.get('result', '').get('cvr', '')),
                ecpm            = str(result.get('result', '').get('ecpm', ''))
            ).__dict__

            for result in results
        ]

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
        