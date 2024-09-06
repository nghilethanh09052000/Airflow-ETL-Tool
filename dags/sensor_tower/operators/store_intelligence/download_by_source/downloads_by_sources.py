from typing import List, Sequence, Dict, Any
from enum import Enum
from airflow.utils.context import Context
from sensor_tower.scripts.utils import MobileOperatingSystem
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from utils.constants import COUNTRY_CODE
from dataclasses import dataclass

@dataclass
class UnifiedAppParams:

    os: MobileOperatingSystem.IOS
    app_id_type: str
    app_ids: str


@dataclass
class DownloadBySourcesParams:
    os: str
    app_ids: str
    countries: str
    start_date: str
    end_date: str

    def __post_init__(self):
    
        if self.os != 'unified':
            raise ValueError('Invalid Operating System. Expected Only Unified')

        if not bool(set(self.countries.split(',')).intersection(list(COUNTRY_CODE.keys())) ):
            raise ValueError('Invalid Country. Please specify countries from the list of COUNTRY_CODE.')


@dataclass
class BreakDownData:
    date: str
    organic_abs: float
    browser_abs: float
    paid_abs: float
    organic_frac: float
    browser_frac: float
    paid_frac: float

@dataclass
class DownloadBySourcesData:
    app_id: str
    breakdown: List[BreakDownData]


"""
https://app.sensortower.com/api/docs/store_intel#/Downloads%20by%20Source/downloads_by_sources
"""


class DownloadsBySourcesEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):
    
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
        os: str,
        app_ids:List[Dict[str, Any]],
        countries: List[str],
        **kwargs
    ):
    
        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/store_intelligence/downloads_by_source/downloads_by_sources",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.ds = ds


        self.app_ids = app_ids
        self.os = os
        self.countries = countries
    
    def _get_apps_os_data(
            self, 
            os: MobileOperatingSystem.IOS
        ) -> str:
        return ','.join([str(app_id[os]) for app_id in self.app_ids if app_id[os]])
    
    def _get_unified_app(
            self,
            apps_data: str
        ) -> str:
        params: UnifiedAppParams = {'os':'unified','app_id_type': 'itunes', 'app_ids': apps_data}
        results = self.api.get_unified_apps_data(base_params=params)
        return ','.join([result.get('unified_app_id') for result in results])
    
    def _get_countries_string(self):
        return ','.join(self.countries)

    def _format_params(
            self,
            unified_apps_data: str,
            countries: str,
            os: str = MobileOperatingSystem.UNIFIED.value
        ):

        return DownloadBySourcesParams(
            os=os,
            app_ids=unified_apps_data,
            countries=countries,
            start_date=self.ds,
            end_date=self.ds
        ).__dict__

    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)
    
    def fetch_data(self):

        unified_apps_data: str = self._get_unified_app(apps_data=self._get_apps_os_data(MobileOperatingSystem.IOS.value))
        countries: str = self._get_countries_string()
        params: DownloadBySourcesParams = self._format_params(unified_apps_data=unified_apps_data, countries=countries)
        results: List[Dict[str,Any]] = self.api.get_downloads_by_sources(base_params=params)

        return self._format_response_result(results=results)
    

    def _format_break_down_data(
            self, 
            items: List[str]
        )-> List[BreakDownData]:

        return [
            {
                'date'        : str(item.get('date')),
                'organic_abs' : float(item.get('organic_abs')),
                'browser_abs' : float(item.get('browser_abs')),
                'paid_abs'    : float(item.get('paid_abs')),
                'organic_frac': float(item.get('organic_frac')),
                'browser_frac': float(item.get('browser_frac')),
                'paid_frac'   : float(item.get('paid_frac'))
            } for item in items
        ]
    
    def _format_response_result(
            self, 
            results: List
        ) -> List[DownloadBySourcesData]:

        if not results: return results

        return [
            {
                'app_id'   : str(result.get('app_id')),
                'breakdown': self._format_break_down_data(items=result.get('breakdown'))
            } for result in results
        ]


