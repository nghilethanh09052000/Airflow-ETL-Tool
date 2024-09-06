from typing import List, Sequence, Any, Dict
from enum import Enum
from airflow.utils.context import Context
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from utils.constants import COUNTRY_CODE
from enum import Enum
from dataclasses import dataclass


class DateGranularity(Enum):
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MOTHLY = 'monthly'
    QUARTERLY = 'quarterly'

@dataclass
class SaleReportEstimateParams:
    os               : str
    app_ids          : str
    countries        : str
    date_granularity : DateGranularity
    start_date       : str
    end_date         : str


    def __post_init__(self):
        
        if self.os not in ['ios', 'android']:
            raise ValueError('Invalid Operating System. Expected Both Android or IOS')

        if not bool(set(self.countries.split(',')).intersection(list(COUNTRY_CODE.keys())) ):
            raise ValueError('Invalid Country. Please specify countries from the list of COUNTRY_CODE.')

        if self.date_granularity not in [data.value for data in DateGranularity]:
            raise ValueError('Invalid Date Granularity. Please specify on the List DateGranularity')

@dataclass
class SaleReportEstimateData:
    aid: str
    cc : str
    d  : str
    au : float
    ar : float
    iu : float
    ir : float


"""
https://app.sensortower.com/api/docs/store_intel#/Download%20and%20Revenue%20Estimates/sales_report_estimates
"""

class SaleReportEstimateEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):

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
        app_ids: List[Dict[str, Any]],
        countries: List[str],
        date_granularity: str,
        **kwargs
    ):

        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/store_intelligence/download_and_revenues_estimates/sales_report_estimates",
            **kwargs
        )

        SensorTowerAbstractEndpoint.__init__(
            self, 
            http_conn_id=http_conn_id
        )

        self.ds = ds

        self.os = os
        self.app_ids = app_ids
        self.countries = countries
        self.date_granularity = date_granularity

    def _format_app_ids(self, os) -> str:
        return ','.join([str(app_id[os]) for app_id in self.app_ids if app_id[os]])
    
    def _get_countries_string(self) -> str:
        return ','.join(self.countries)

    def _format_params(
            self,
            os: str
        ) -> SaleReportEstimateParams:

        return SaleReportEstimateParams(
            os = os,
            app_ids = self._format_app_ids(os=os),
            countries = self._get_countries_string(),
            date_granularity = self.date_granularity,
            start_date = self.ds,
            end_date = self.ds
        ).__dict__
    
    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)

    def fetch_data(self):
        results = []
        for os in self.os:
            params = self._format_params(os)
            data = self.api.get_sale_report_estimates(base_params=params)
            results.extend(data)

        return self._format_response_result(results=results)
    

    def _format_response_result(
            self, 
            results: List
        ) -> List[SaleReportEstimateData]:
                
        if not results: return results

        return [
            {
                'aid' : str(result.get('aid')),
                'cc'  : str(result.get('cc')),
                'd'   : str(result.get('d')),
                'au'  : result.get('au'),
                'ar'  : result.get('ar'),
                'iu'  : result.get('iu'),
                'ir'  : result.get('ir')
            } for result in results
        ]