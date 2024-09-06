from typing import Any, Sequence, List, Union, Dict
from sensor_tower.operators.base import SensorTowerBaseOperator
from .unified_lookup import UnifiedAppsEndpoint
from .apps import AppsEndpoint
from .reviews import GetReviewsEndpoint

class AppIntelligenceEndpointFactory:

    def __init__(self, ds, http_conn_id):
        self.ds = ds
        self._http_conn_id = http_conn_id

    @property
    def get_reviews_endpoint(self):
        return GetReviewsEndpoint

    @property
    def apps_endpoint(self):
        return AppsEndpoint
    
    @property
    def unified_app_endpoint(self):
        return UnifiedAppsEndpoint
    
    def get_reviews_filter(
            self,
            app_id: Dict[str, str],
            country: List,
            rating_filter: Union[int, str],
            search_term: str,
            username: str,
            limit: Union[str, int],
            page: Union[str, int]
        ):
        return GetReviewsEndpoint(self.ds, self._http_conn_id) \
                .filter \
                    .app_id(app_id) \
                    .country(country) \
                    .rating_filter(rating_filter) \
                    .search_term(search_term) \
                    .username(username) \
                    .limit(limit) \
                    .page(page)
    
                    
                    
    
    def apps_filter(
            self,
            country: str
        ):  
        return AppsEndpoint(self.ds, self._http_conn_id) \
                .filter \
                    .country(country)
    
    def unified_app_filter(
            self
    ):
        return UnifiedAppsEndpoint(self.ds, self._http_conn_id)
                
    
class AppIntelligenceOperator(SensorTowerBaseOperator):

    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )
    def __init__(
            self,
            task_id: str,
            endpoint_class,
            endpoint_filter,
            ds,
            http_conn_id,
            gcs_bucket: str, 
            gcs_prefix: str,
            **kwargs
        ):

        super().__init__(
            ds=ds,
            task_id=task_id, 
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            **kwargs
        )

        self._http_conn_id = http_conn_id
        self._endpoint_class = endpoint_class    
        self._endpoint_filter = endpoint_filter
    
    def execute(self, context: Any):
        if not self._endpoint_class:
            raise ValueError(f'Invalid Endpoint: {self._endpoint}')
        
        data = self._endpoint_class(self.ds, self._http_conn_id).fetch_data()
        self._upload_data(data)