from typing import Dict

import requests
import tenacity
from airflow.providers.http.hooks.http import HttpHook
from utils.data_extract.http_requests import RETRIES_NUM


class OpenExchangeBaseHook(HttpHook):
    def __init__(self, **kwargs):
        super().__init__(method="GET", **kwargs)

    def make_http_request(self, endpoint: str, params: Dict = None, **kwargs):
        retry_args = dict(
            wait=tenacity.wait_exponential(),
            stop=tenacity.stop_after_attempt(RETRIES_NUM),
            retry=tenacity.retry_if_exception_type(
                (requests.exceptions.ConnectionError)
            ),
        )

        response: requests.Response = self.run_with_advanced_retry(
            endpoint=endpoint, data=params, _retry_args=retry_args
        )
        return response
