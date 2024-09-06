from typing import Dict, List, Sequence, Any,  Literal
from airflow.utils.context import Context
from sensor_tower.operators.endpoint import SensorTowerAbstractEndpoint
from sensor_tower.operators.base import SensorTowerBaseOperator
from dataclasses import dataclass
from utils.constants import COUNTRY_CODE


@dataclass
class GetReviewsParams:
    os: str
    app_id: str
    start_date: str
    end_date: str
    country: str
    rating_filter: str
    search_term: str
    username: str
    limit: int
    page: int

    def __post_init__(self):
        
        if self.os not in ['ios', 'android']:
            raise ValueError('Invalid Operating System. Expected Both Android or IOS')

        if self.country not in list(COUNTRY_CODE.keys()):
            raise ValueError('Invalid Country. Please specify a country from the list of', list(COUNTRY_CODE.keys()))

        if self.limit and (self.limit < 0 or self.limit > 200):
            raise ValueError('Invalid Limit number specified. Limit must be a specific number or not be exceed 200')

        if self.page and (self.page < 0):
            raise ValueError('Invalid Limit Page Specified. Page Number Must be a positive number')


@dataclass
class GetReviewsData:
    app_id   : str
    content  : str
    title    : str
    tags     : List[str]
    version  : str
    country  : str
    username : str
    date     : str
    rating   : int


class GetReviewsEndpointOperator(SensorTowerBaseOperator, SensorTowerAbstractEndpoint):
    
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
        app_ids: Dict[str , Literal['android', 'ios']],
        countries: List[str],
        rating_filter: str,
        search_term: str,
        username: str,
        limit: int,
        page: int,
        **kwargs
    ):
    
        SensorTowerBaseOperator.__init__(
            self,
            ds=ds,
            task_id=task_id,
            gcs_bucket=gcs_bucket,
            gcs_prefix=f"{gcs_prefix}/app_intelligence/reviews/get_reviews",
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
        self.rating_filter = rating_filter
        self.search_term = search_term
        self.username = username
        self.limit = limit
        self.page = page

    def _format_app_id(
            self,
            os: str,
            app_ids: Dict[str, str]
        ) -> List[str]:
            return [name for name, platform in app_ids.items() if platform == os]

   
    def execute(self, context: Context):
        data = self.fetch_data()
        self._upload_data(data=data)

    def fetch_data(self):

        results = []

        for os in self.os:
            os_app_ids: List[str] = self._format_app_id(os = os, app_ids=self.app_ids)  
            for os_app_id in os_app_ids:
                for country in self.countries:
                    params = self._format_params(os = os, app_id=os_app_id, country=country)
                    data = self.api.get_reviews(base_params=params)
                    results.extend(data)

        return self._format_response_result(results=results)
    
    def _format_params(
            self,
            os: str,
            app_id: str,
            country: str
        ) -> GetReviewsParams:

        params = {
            'os'            : os,
            'app_id'        : app_id,
            'start_date'    : self.ds,
            'end_date'      : self.ds,
            'country'       : country,
            'rating_filter' : self.rating_filter,
            'search_term'   : self.search_term,
            'username'      : self.username,
            'limit'         : self.limit,
            'page'          : self.page
        }

        return GetReviewsParams(**params).__dict__

    
    def _format_response_result(
            self, 
            results: List
        ) -> List[GetReviewsData]:
        if not results: return results
        return [
            {
                'app_id'   : str(result.get('app_id')),
                'content'  : result.get('content'),
                'title'    : str(result.get('title')),
                'tags'     : result.get('tags'),
                'version'  : str(result.get('version')),
                'country'  : result.get('country'),
                'username' : result.get('username'),
                'date'     : result.get('date'),
                'rating'   : int(result.get('rating'))
            } for result in results
        ] 