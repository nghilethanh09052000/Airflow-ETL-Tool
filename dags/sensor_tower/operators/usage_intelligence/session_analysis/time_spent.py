from typing import List, Dict, Any, Sequence
from enum import Enum
from airflow.utils.context import Context
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from dataclasses import dataclass
from enum import Enum

class DateGranularity(Enum):
    ALL_TIME = 'all_time'
    QUARTERLY = 'quaterly'
    MONTHLY = 'monthly'


@dataclass
class TimeSpentParams:
    os: str
    app_ids: str
    date_granularity: str
    start_date: str
    end_date: str

    def __post_init__(self):

        if self.os not in ['ios', 'android']:
            raise ValueError('Invalid Operating System. Expected Both Android or IOS')
        
        if self.date_granularity not in [data.value for data in DateGranularity]:
            raise ValueError('Invalid Date Granularity. Please specify "all_time", "monthly" "quarterly".')

@dataclass
class TimeSpentData:
    app_id: str
    confidence: int
    country: str
    date_granularity: str
    date: str
    end_date: str
    grouped_normalized_time_spent_3: float
    grouped_normalized_time_spent_10: float
    grouped_normalized_time_spent_30: float
    grouped_normalized_time_spent_60: float
    grouped_normalized_time_spent_180: float
    grouped_normalized_time_spent_600: float
    grouped_normalized_time_spent_1800: float
    grouped_normalized_time_spent_3600: float
    grouped_normalized_time_spent_36000: float
    grouped_normalized_time_spent_36001: float
    average_time_spent_per_user: float
    time_period: str


"""
https://app.sensortower.com/api/docs/usage_intel#/Session%20Analysis/app_analysis_time_spent
"""

class FilterTimePeriodDataResult:

    def __init__(
        self,
        type: str,
        items: Dict[str, List]
    ):
        self._type = type
        self._items = items

    def get_data(self):
        return self._items.get(self._type)

       

class TimeSpentEndPointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):
    
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
        date_granularity:str,
        response_type:str,
        **kwargs
    ):

        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/usage_intelligence/session_analysis/time_spent",
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
        self.response_type = response_type
    
    def _format_app_ids(self, os) -> str:
        return ','.join([str(app_id[os]) for app_id in self.app_ids if app_id[os]])

    def _format_params(self, os:str):

        app_ids = self._format_app_ids(os)
        date_granularity = self.date_granularity
        start_date = self.ds
        end_date = self.ds,

        return TimeSpentParams(
            os=os,
            app_ids=app_ids,
            date_granularity=date_granularity,
            start_date=start_date,
            end_date=end_date
        ).__dict__
    
    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)

    def fetch_data(self):
        results = []
        params = {}
        for os in self.os:
            params = self._format_params(os)
            items = self.api.get_time_spent_data(base_params=params)
            data = FilterTimePeriodDataResult(
                            type=self.response_type,
                            items=items
                        ).get_data()
            
            results.extend(data)
         
        return self._format_response_result(results=results)
    
    def _format_response_result(
            self, 
            results: List[Dict[str, Any]]
        ):

        if not results: return results   

        return [
            {
                'app_id'                              : str(result.get('app_id')),
                'confidence'                          : str(result.get('confidence')),
                'country'                             : str(result.get('country')),
                'date_granularity'                    : str(result.get('date_granularity')),
                'date'                                : str(result.get('date')),
                'end_date'                            : str(result.get('end_date')),
                'grouped_normalized_time_spent_3'     : float(result.get('grouped_normalized_time_spent').get('3')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_10'    : float(result.get('grouped_normalized_time_spent').get('10')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_30'    : float(result.get('grouped_normalized_time_spent').get('30')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_60'    : float(result.get('grouped_normalized_time_spent').get('60')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_180'   : float(result.get('grouped_normalized_time_spent').get('180')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_600'   : float(result.get('grouped_normalized_time_spent').get('600')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_1800'  : float(result.get('grouped_normalized_time_spent').get('1800')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_3600'  : float(result.get('grouped_normalized_time_spent').get('3600')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_36000' : float(result.get('grouped_normalized_time_spent').get('36000')) if result.get('grouped_normalized_time_spent') else None,
                'grouped_normalized_time_spent_36001' : float(result.get('grouped_normalized_time_spent').get('36001')) if result.get('grouped_normalized_time_spent') else None,
                'average_time_spent_per_user'         : float(result.get('average_time_spent_per_user')),
                'time_period'                         : str(result.get('time_period')),
            } for result in results
        ]


