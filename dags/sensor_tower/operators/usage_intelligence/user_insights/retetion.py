from typing import List, Dict, Any, Sequence
from enum import Enum
from airflow.utils.context import Context
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from dataclasses import dataclass



class MobileOperatingSystem(Enum):
    IOS = 'ios'
    ANDROID = 'android'
class DateGranularity(Enum):
    ALL_TIME = 'all_time'
    QUARTERLY = 'quarterly'

REQUIRED_COUNTRY_CODES = {
        "AU": "Australia",
        "BR": "Brazil",
        "CA": "Canada",
        "DE": "Germany",
        "ES": "Spain",
        "FR": "France",
        "GB": "United Kingdom",
        "IN": "India",
        "IT": "Italy",
        "JP": "Japan",
        "KR": "South Korea",
        "US": "US"
}

@dataclass
class RententionParams:
    os: str
    app_ids: str
    date_granularity: DateGranularity
    start_date: str
    end_date: str
    country: str

    def __post_init__(self):
        
        if self.os not in ['ios', 'android']:
            raise ValueError('Invalid Operating System. Expected Both Android or IOS')

        if self.country != 'WW' and self.country not in REQUIRED_COUNTRY_CODES:
            raise ValueError('Invalid Country. Please specify a country from the list of REQUIRED_COUNTRY_CODES.')

        if self.date_granularity not in [data.value for data in DateGranularity]:
            raise ValueError('Invalid Date Granularity. Please specify "all_time" or "quarterly".')

@dataclass
class RetentionData:
    app_id: str
    confidence: int
    country: str
    date_granularity: str
    date: str
    end_date: str
    corrected_retention: List[float]
  
"""
https://app.sensortower.com/api/docs/usage_intel#/User%20Insights/app_analysis_retention
"""


class RetentionEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):
    
    def __init__(
        self,
        ds: str,
        task_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        http_conn_id: str,
        os: List[str],
        app_ids: str,
        date_granularity,
        country,
        **kwargs
    ):

        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/usage_intelligence/user_insights/retention",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.ds = ds

        self.os = os
        self.app_ids = app_ids
        self.date_granularity = date_granularity
        self.country = country

    def _format_app_ids(self, os) -> str:
        return ','.join([str(app_id[os]) for app_id in self.app_ids if app_id[os]])

    def _format_params(
        self,
        os: str
    ) -> RententionParams:

        app_ids = self._format_app_ids(os)
        date_granularity = self.date_granularity
        start_date = self.ds,
        end_date = self.ds,
        country = self.country

        return RententionParams(
            os=os,
            app_ids=app_ids,
            date_granularity=date_granularity,
            start_date=start_date,
            end_date=end_date,
            country=country
        ).__dict__

    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)
    
    def fetch_data(self):
        results = []
        for os in self.os:
            params = self._format_params(os=os)
            data = self.api.get_retention_data(base_params=params)
            results.extend(data)

        if not results: return results

        return self._format_response_result(results=results)
    
    def _format_response_result(
        self,
        results: List[Dict[str, Any]]
    ) -> List[RetentionData]:
        return [
            {
                'app_id'              : str(result.get('app_id')),
                'confidence'          : result.get('confidence'),
                'country'             : result.get('country'),
                'date_granularity'    : result.get('date_granularity'),
                'date'                : result.get('date'),
                'end_date'            : result.get('end_date'),
                'corrected_retention' : result.get('corrected_retention')
            } for result in results
        ]

    
