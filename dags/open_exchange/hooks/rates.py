from typing import Union
from datetime import date

from open_exchange.hooks.base import OpenExchangeBaseHook


class OpenExchangeRatesHook(OpenExchangeBaseHook):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.app_id = self.get_connection(self.http_conn_id).extra_dejson["app_id"]

    def get_latest(
        self, base: str = "USD", symbols: str = None, prettyprint: bool = True,
    ):
        """
        Get the latest exchange rates available from the Open Exchange Rates API.

        ..seealso:
            https://docs.openexchangerates.org/reference/latest-json
        """
        endpoint = "/api/latest.json"
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "app_id": self.app_id,
                "base": base,
                "symbols": symbols,
                "prettyprint": prettyprint,
            },
        )
        self.check_response(response)
        return response.json()

    def get_historical(
        self,
        date: Union[date, str],
        base: str = "USD",
        symbols: str = None,
        prettyprint: bool = True,
    ):
        """
        Get historical exchange rates for any date available from the Open Exchange Rates API, currently going back to 1st January 1999.

        ..seealso:
            https://docs.openexchangerates.org/reference/historical-json
        """
        endpoint = f"/api/historical.{date}.json"
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "app_id": self.app_id,
                "base": base,
                "symbols": symbols,
                "prettyprint": prettyprint,
            },
        )
        self.check_response(response)
        return response.json()
