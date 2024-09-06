from typing import Dict
from sensor_tower.hooks.base import SensorTowerBaseHook
from sensor_tower.scripts.utils import (
    MobileOperatingSystem
)
class SensorTowerApiHook(SensorTowerBaseHook):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = self.get_connection(self.http_conn_id).extra_dejson["api_key"]
    
    def _format_endpoint(
            self, 
            os: MobileOperatingSystem, 
            endpoint: str
        ):
        return f'/v1/{os}/{endpoint}'
    
    def _format_params(
            self, 
            params: Dict
        ):
        del params['os']
        return {**params, 'auth_token': self.api_key}

    def _Request(
        self,
        api_endpoint,
        api_params
    ):
        endpoint = self._format_endpoint(api_params.get('os'), api_endpoint)
        params = self._format_params(api_params)
        try:
            response = self.make_http_request(endpoint=endpoint, params=params)
            self.check_response(response)
            return response
        except ValueError as e:
            raise(f"Response Error as: {e} - {endpoint}",)

    """https://app.sensortower.com/api/docs/app_intel"""
    def get_apps_data(self, base_params):
        base_endpoint = 'apps'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['apps']
        return data
    
    def get_reviews(self, base_params):
        base_endpoint = 'review/get_reviews'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['feedback']
        return data

    def get_unified_apps_data(self, base_params):
        """/unified/apps"""
        base_endpoint = 'apps'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['apps']
        return data

    """https://app.sensortower.com/api/docs/ad_intel"""
    def get_creatives_tops_data(self, base_params):
        base_endpoint = 'ad_intel/creatives/top'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['ad_units']
        return data
    
    """https://app.sensortower.com/api/docs/usage_intel"""
    def get_active_users_data(self, base_params):
        base_endpoint = 'usage/active_users'
        response = self._Request(base_endpoint, base_params)
        data = response.json()
        return data
    
    def get_demographic_data(self, base_params):
        base_endpoint = 'usage/demographics'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['app_data']
        return data
    
    def get_retention_data(self, base_params):
        base_endpoint = 'usage/retention'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['app_data']
        return data
        
    def get_app_overlap_data(self, base_params):
        base_endpoint = 'usage/app_overlap'
        response = self._Request(base_endpoint, base_params)
        data = response.json()
        return data
    
    def get_time_spent_data(self, base_params):
        base_endpoint = 'usage/time_spent'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['app_data']
        return data
    
    """https://app.sensortower.com/api/docs/store_intel#/"""
    def get_sales_report_estimates_comparison_attributes(self, base_params):
        base_endpoint = 'sales_report_estimates_comparison_attributes'
        response = self._Request(base_endpoint, base_params)
        data = response.json()
        return data

    def get_sale_report_estimates(self, base_params):
        base_endpoint = 'sales_report_estimates'
        response = self._Request(base_endpoint, base_params)
        data = response.json()
        return data
    
    def get_downloads_by_sources(self, base_params):
        base_endpoint = 'downloads_by_sources'
        response = self._Request(base_endpoint, base_params)
        data = response.json()['data']
        return data