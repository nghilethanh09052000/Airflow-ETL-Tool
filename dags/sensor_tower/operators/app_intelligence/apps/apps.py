from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from typing import Dict, List, Sequence
from airflow.utils.context import Context
from utils.constants import COUNTRY_CODE
from dataclasses import dataclass
from enum import Enum
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from google.cloud import bigquery
import random
import time
import logging
class MobileOperatingSystem(Enum):
    IOS = 'ios'
    ANDROID = 'android'

@dataclass
class AppsParams:
    os: str
    app_ids: str
    country: str

    def __post_init__(self):
        
        if self.os not in [data.value for data in MobileOperatingSystem]:
            raise ValueError('Invalid Operating System. Expected:', [data.value for data in MobileOperatingSystem])

        if self.country not in list(COUNTRY_CODE.keys()):
            raise ValueError('Invalid Country. Please specify a country from the list of REQUIRED_COUNTRY_CODES.')

@dataclass
class AppsData:
    app_id: str
    name: str
    humanized_name: str 
    publisher_id: str
    publisher_name: str
    os: str


class AppsEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):

    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds"
    )

    def __init__(
        self,
        ds: str,
        task_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        http_conn_id: str,
        os: List[str],
        country: str,
        **kwargs
    ):

        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/app_intelligence/apps/apps",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
        self.BIGQUERY_PROJECT = Variable.get("bigquery_project")
        self.BQ_DATASET = "raw_sensortower"

        self.ds = ds 
        self.os = os
        self.country = country
    
    
    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)   
    
    def fetch_data(self):
        results = []
        os_app_ids = self._get_app_ids_from_bq()
        for os in self.os:
            params = self._format_params(os=os, os_app_ids=os_app_ids)
            data = self.api.get_apps_data(base_params=params)
            results.extend(data)
        return self._format_response_result(results=results)

    def _get_app_ids_from_bq(self):
        max_retries = 3
        retry_delay_seconds = 60 

        for retry in range(1, max_retries + 1):
            try:
                query = f"""
                    SELECT DISTINCT 
                        app_id
                    FROM 
                        `{self.BIGQUERY_PROJECT}.{self.BQ_DATASET}.raw_store_intelligence_sales_report_estimates_comparison_attributes`
                    ;
                 """
                bq_client = bigquery.Client.from_service_account_json(self.service_account_json_path)
                query_job = bq_client.query(query, project=self.BIGQUERY_PROJECT)
                app_ids = [row.get('app_id') for row in query_job]
                return random.sample(app_ids, min(100, len(app_ids)))
            except Exception as e:
                logging.info(f"Error executing BigQuery query: {str(e)}")
                if retry < max_retries:
                    logging.info(f"Retrying in {retry_delay_seconds} seconds (Retry {retry}/{max_retries})...")
                    time.sleep(retry_delay_seconds)
                else:
                    raise ValueError("Max retries reached. Unable to get app IDs.")
        
    
    def _format_app_ids(
            self, 
            os:str,
            os_app_ids: List[str]
        ) -> str:
        return ','.join([ os_app_id for os_app_id in os_app_ids if os_app_id.isdigit()]) \
                    if os == MobileOperatingSystem.IOS.value \
                        else ','.join([os_app_id for os_app_id in os_app_ids if not os_app_id.isdigit() ])
    
    
    def _format_params(
        self, 
        os: str,
        os_app_ids: List[str]
    ) -> Dict:
        
        return AppsParams(
            os=os,
            app_ids=self._format_app_ids(os=os, os_app_ids = os_app_ids),
            country=self.country
        ).__dict__

    def _format_response_result(
            self, 
            results: List
        ) -> List[AppsData]:

        if not results: return results

        return [
            {
                'app_id'        : str(result.get('app_id')),
                'name'          : str(result.get('name')),
                'humanized_name': str(result.get('humanized_name')),
                'publisher_id'  : str(result.get('publisher_id')),
                'publisher_name': str(result.get('publisher_name')),
                'os'            : str(result.get('os'))
            } for result in results
        ] 