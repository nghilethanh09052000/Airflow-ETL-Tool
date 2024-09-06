from typing import Any, Sequence, List, Union
from sensor_tower.operators.base import SensorTowerBaseOperator

from .download_and_revenue_estimates import SaleReportEstimateEndpoint
from .download_by_source import DownloadsBySourcesEndpoint
from .top_apps import (
    SalesReportEstimatesCompareAttrEndpoint
)


"""
    LIST API: https://app.sensortower.com/api/docs/store_intel#/
"""


class StoreIntelligenceEndpointsFactory:

    def __init__(self, ds, http_conn_id):
        self.ds = ds
        self._http_conn_id = http_conn_id

    @property
    def sales_report_estimates_comparison_endpoint(self):
        return SalesReportEstimatesCompareAttrEndpoint
    
    @property
    def sales_report_estimates_endpoint(self):
        return SaleReportEstimateEndpoint
    
    @property 
    def downloads_by_sources_endpoint(self):
        return DownloadsBySourcesEndpoint  

    def sales_report_estimates_comparison_filter(
            self,
            os: List,
            comparison_attribute: str,
            time_range: str,
            measure: str,
            ios_device: str,
            android_device,
            ios_category: int,
            android_category: str,
            country: str,
            limit: Union[str,int],
            offset: Union[str,int],
            format_response_type: str
        ):
        return SalesReportEstimatesCompareAttrEndpoint(self.ds, self._http_conn_id) \
            .filter \
                .os(os) \
                .comparison_attribute(comparison_attribute) \
                .time_range(time_range) \
                .measure(measure) \
                .ios_device(ios_device) \
                .android_device(android_device) \
                .ios_category(ios_category) \
                .android_category(android_category) \
                .country(country) \
                .limit(limit) \
                .offset(offset) \
                .format_response_type(format_response_type)

    def sales_report_estimates_filter(
            self,
            date_granularity: str,
            countries: List
        ):
        return SaleReportEstimateEndpoint(self.ds, self._http_conn_id) \
                    .filter \
                        .date_granularity(date_granularity) \
                        .countries(countries)          
    
    def downloads_by_sources_filter(
            self,
            countries
        ):
        return DownloadsBySourcesEndpoint(self.ds, self._http_conn_id) \
                    .filter \
                        .countries(countries)
    
class StoreIntelligenceOperator(SensorTowerBaseOperator):
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
