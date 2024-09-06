import functools
import logging
from typing import Dict

import requests
import tenacity

from airflow.providers.http.hooks.http import HttpHook
from utils.data_extract.http_requests import RETRIES_NUM


class FacebookBaseHook(HttpHook):
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

        try:
            response: requests.Response = self.run_with_advanced_retry(
                endpoint=endpoint, data=params, _retry_args=retry_args
            )
            if response.status_code == 400:
                logging.warning("Received HTTP 400 (Bad Request). Skipping processing.")
                return None

            return response

        except Exception as e:
            logging.error(f"An error occurred: {e}. Skipping processing.")
            return None

    @staticmethod
    def paginator(func):
        def next_page(response, *args, **kwargs):
            self = args[0]
            response_json = response.json()
            paging = response_json.get("paging")
            if not paging.get("next"):
                return lambda: (None, None)

            after = paging.get("cursors").get("after")

            def call():
                logging.info("next page args: %s", args[1:])
                next_kwargs = {**kwargs, **{"after": after}}
                logging.info("next page kwargs: %s", next_kwargs)
                return getattr(self, func.__name__)(*args[1:], **next_kwargs,)

            return call

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            response: requests.Response = func(*args, **kwargs)
            logging.info("current page args: %s", args)
            logging.info("current page kwargs: %s", kwargs)
            if response is not None and "paging" in response.json():
                return response, next_page(response, *args, **kwargs)
            else:
                return (response, None)
            
        return wrapper
