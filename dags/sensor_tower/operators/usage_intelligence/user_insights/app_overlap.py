from typing import List, Union
from airflow.utils.context import Context
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from sensor_tower.scripts.utils import DateGranularity
from dataclasses import dataclass
from enum import Enum





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

class OverLapPeriod(Enum):
    ONE_DAY = '1_day'
    SEVEN_DAY = '7_day'
    THIRTY_DAY = '30_day'

class QuantitativeMeasurement(Enum):
    STRONG = 'strong'
    SOMEWHAT_STRONG = 'somewhat_strong'
    MODERATE = 'moderate'
    SOMEWHAT_WEAK = 'somewhat_weak'
    WEAK = 'weak'

class DateGranularity(Enum):
    ALL_TIME = 'all_time'
    QUARTERLY = 'quarterly'


@dataclass
class OverlapApp:
    app_id: str
    increase_chance_of_use: float

@dataclass
class OverLapParams:
    os                          : str
    app_ids                     : str
    period                      : str
    filter_off_small_apps       : str
    country_distributions_match : str
    date_granularity            : str
    date                        : str

    def __post_init__(self):
        
        if self.os not in ['ios', 'android']:
            raise ValueError('Invalid Operating System. Expected Both Android or IOS')
        
        if self.period not in [data.value for data in OverLapPeriod]:
            raise ValueError('Period Must Be Specifed On This List', OverLapPeriod)
        
        if self.filter_off_small_apps not in [data.value for data in QuantitativeMeasurement]:
            raise ValueError('Filter Of Small App Must Be Specifed On This List', QuantitativeMeasurement)
        
        if self.country_distributions_match not in [data.value for data in QuantitativeMeasurement]:
            raise ValueError('Filter Of Small App Must Be Specifed On This List', QuantitativeMeasurement)

        if self.date_granularity not in [data.value for data in DateGranularity]:
            raise ValueError('Invalid Date Granularity. Please specify "all_time" or "quarterly".')
    

@dataclass
class OverLapData:
    app_id           : str
    country          : str
    start_date       : str
    overlap_period   : str
    date_granularity : str
    confidence       : int
    gain_a_to_b      : List[OverlapApp]
    gain_b_to_a      : List[OverlapApp]



"""
https://app.sensortower.com/api/docs/usage_intel#/User%20Insights/usage_app_overlap
"""       


class AppOverLapEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):


    def __init__(
        self,
        ds: str,
        task_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        http_conn_id: str,
        os: List[str],
        app_ids: str,
        period: str,
        filter_off_small_apps: str,
        country_distributions_match: str,
        date_granularity,
        **kwargs
    ):

        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/usage_intelligence/user_insights/app_overlap",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.os = os
        self.app_ids = app_ids
        self.period = period
        self.filter_off_small_apps = filter_off_small_apps
        self.country_distributions_match = country_distributions_match
        self.date_granularity = date_granularity



    def _format_app_ids(self, os) -> str:
        return ','.join([str(app_id[os]) for app_id in self.app_ids if app_id[os]])
    
    def _format_params(
            self,
            os: str
        ) -> OverLapParams:
        return OverLapParams(
            os                          = os,
            app_ids                     = self._format_app_ids(os),
            period                      = self.period,
            filter_off_small_apps       = self.filter_off_small_apps,
            country_distributions_match = self.country_distributions_match,
            date_granularity            = self.date_granularity,
            date                        = self.ds
        ).__dict__
       

    def _format_overlap_app(
            self,
            items: List[OverlapApp]
        ) -> List[OverlapApp]: 
        return [
            {
                'app_id': str(app_id),
                'increase_chance_of_use': value
            } for app_id, value in items
        ]
    
    def _format_response_result(
            self, 
            results: List[Union[str, float]]
        ) -> List[OverLapData]:

        return [
            {
                'app_id'           : str(result.get('app_id')),
                'country'          : str(result.get('country')),
                'start_date'       : str(result.get('result')),
                'overlap_period'   : str(result.get('overlap_period')),
                'date_granularity' : str(result.get('date_granularity')),
                'confidence'       : int(result.get('confidence')),
                'gain_a_to_b'      : self._format_overlap_app(result.get('gain_a_to_b')),
                'gain_b_to_a'      : self._format_overlap_app(result.get('gain_b_to_a'))

            } for result in results
        ]
    
    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)
    
    def fetch_data(self):
        results = []
        for os in self.os:
            params = self._format_params(os=os)
            data = self.api.get_app_overlap_data(base_params=params)
            results.append(data)

        if not results: return results

        return self._format_response_result(results=results)
    

