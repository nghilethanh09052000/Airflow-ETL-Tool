from typing import List, Sequence
from dataclasses import dataclass
from airflow.utils.context import Context
from enum import Enum
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from sensor_tower.scripts.utils import MobileOperatingSystem
from utils.constants import COUNTRY_CODE



class TimePeriod(Enum):
    DAY = 'day'
    MONTH = 'month'
    WEEK = 'week'

@dataclass
class ActiveUsersParams:
    os          : str
    app_ids     : str
    time_period : str
    start_date  : str
    end_date    : str
    countries   : str

    def __post_init__(self):
        
        if self.os not in [data.value for data in MobileOperatingSystem]:
            raise ValueError('Invalid Operating System. Expected Android, IOS, Unified')
        
        if self.time_period not in [data.value for data in TimePeriod]:
            raise ValueError('Invalid Time Period. Expected Day, Month, Week')

        if not bool(set(self.countries.split(',')).intersection(list(COUNTRY_CODE.keys())) ):
            raise ValueError('Invalid Country. Please specify countries from the list of COUNTRY_CODE.')


@dataclass
class ActiveUsersEndpointData:
    app_id       : str
    country      : str
    date         : str
    ipad_users   : int
    iphone_users : int

"""
https://app.sensortower.com/api/docs/usage_intel#/Usage%20Active%20Users/usage_active_users
"""

class ActiveUsersEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):
    
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
        app_ids: str,
        time_period: str,
        countries: List[str],
        **kwargs
    ):

        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/usage_intelligence/usage/active_users",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.ds = ds

        self.os = os
        self.app_ids = app_ids
        self.time_period = time_period
        self.countries = countries
     
    def _format_app_ids(self, os) -> str:
        return ','.join([str(app_id[os]) for app_id in self.app_ids if app_id[os]])
    
    def _get_countries_string(self) -> str:
        return ','.join(self.countries)
    
    def _format_params(
            self, 
            os: str
        ) -> ActiveUsersParams:

        app_ids     = self._format_app_ids(os)
        time_period = self.time_period
        start_date  = self.ds
        end_date    = self.ds
        countries   = self._get_countries_string()

        return ActiveUsersParams(
            os=os,
            app_ids=app_ids,
            time_period=time_period,
            start_date=start_date,
            end_date=end_date,
            countries=countries
        ).__dict__

    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)
    
    def fetch_data(self):
        results = []
        for os in self.os:
            params = self._format_params(os=os)
            data = self.api.get_active_users_data(base_params = params)
            results.extend(data)

        return self._format_response_result(results=results)

    def _format_response_result(self, results: List) -> List[ActiveUsersEndpointData]:
        if not results: return results
        return [
            {
                'app_id'       : str(result.get('app_id')),
                'country'      : result.get('country'),
                'date'         : result.get('date'),
                'ipad_users'   : result.get('ipad_users'),
                'iphone_users' : result.get('iphone_users')
            } for result in results
        ]