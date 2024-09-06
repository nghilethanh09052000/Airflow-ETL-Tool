import csv
import json
import io
import requests
import pandas as pd
from max_mediation.hooks.base import MaxMediationBaseHook
from max_mediation.scripts.max_utils import Params


class MaxMediationReportingHook(MaxMediationBaseHook):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.api_key = self.get_connection(self.http_conn_id).extra_dejson["api_key"]

    def get_ad_revenue(self, ds):
        """
        Get the aggregated Ad Revenue data from Max Mediation

        ..seealso:
            https://dash.applovin.com/documentation/mediation/reporting-api/max-ad-revenue
        """
        endpoint = f"/maxReport"
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "api_key": self.api_key,
                "start": ds,
                "end": ds,
                "columns": Params.AD_REVENUE_COLUMNS,
                "format": 'json',
            },
        )
        self.check_response(response)
        return response.json()['results']
    
    def get_download_link(self, ds: str, package_name: str, platform: str):
        endpoint = f"/max/userAdRevenueReport"
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "aggregated": "false",
                "api_key": self.api_key,
                "application": package_name,
                "date": ds,
                "platform": platform,
            },
        )
        print('need imporving and investigating')
        self.check_response(response)
        return response.json()['ad_revenue_report_url']
     
    def get_user_ad_revenue(self, ds, package_name, platform):
        endpoint = self.get_download_link(ds, package_name, platform)
        response = requests.get(
            url=endpoint
        )

        self.check_response(response)
        reader =  csv.DictReader(io.StringIO(response.text))
        json_data = json.dumps(list(reader))
        return json.loads(json_data)
    
    def get_cohort(self, ds, endpoint, columns):
        """
        Get the Cohort data from Max Mediation
        ..seealso:
            https://dash.applovin.com/documentation/mediation/reporting-api/max-cohort
        """
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "api_key": self.api_key,
                "start": ds,
                "end": ds,
                "columns": columns,
                "format": 'json'
            },
        )
        self.check_response(response)

        data = response.json()['results']
        results = pd.DataFrame(data, columns = columns.split(','))
        return results.to_dict(orient='records')
