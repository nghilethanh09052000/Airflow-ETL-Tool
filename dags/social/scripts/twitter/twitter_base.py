import logging
from abc import ABC, abstractmethod

import pandas as pd
from airflow.hooks.base import BaseHook
from requests_oauthlib import OAuth1

from utils.data_extract.http_requests import HTTPRequestsDataExtract
from utils.data_upload.bigquery_upload import BigQueryDataUpload


class TwitterExtract(ABC):
    """
    A class to implement Twitter API v2
    Link docs: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/introduction

    Methods
    -------------
    create_url
        return the full url to send http requests to
    get_params
        return the parameters of the http requests
    bearer_oauth
        Authenticate with Twitter bearer token
    pull_data
        Send http requests and handle pagination
    """

    access = None
    base_url = "https://api.twitter.com/2"
    raw_data = []

    def __init__(self, account, **kwargs) -> None:
        self.account = account

    @abstractmethod
    def create_url():
        pass

    @abstractmethod
    def get_params():
        pass

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {self.access['bearer_token']}"
        r.headers["User-Agent"] = "v2FollowersLookupPython"
        return r

    def oauth1(self):
        "Authenticate by OAuth 1.0a to get non-public metrics"
        return OAuth1(
            client_key=self.access["consumer_key"],
            client_secret=self.access["consumer_secret"],
            resource_owner_key=self.access["access_token"],
            resource_owner_secret=self.access["access_token_secret"],
        )

    def customize_retries_config(self):
        """The Twitter reset the window per 15 minutes, customize the number of retries + backoff factors"""
        logging.info("Customize retries config")
        self.extractor.retries_config.update({"backoff_factor": 15, "total": 10})
        self.extractor.set_up_retries_time_out()

    def initiate_extractor(self, auth_method="bearer"):
        url = self.create_url()
        params = self.get_params()
        if auth_method == "oauth1":
            auth = self.oauth1()
        else:
            auth = self.bearer_oauth
        self.extractor = HTTPRequestsDataExtract(url=url, params=params, auth=auth)
        self.customize_retries_config()

    def pull_data(self):
        self.initiate_extractor()
        ...

    @classmethod
    def airflow_callable(cls, account, **kwargs):
        ins = cls(account, **kwargs)
        ins.pull_data()
        ins.upload_data()


class TwitterUpload(ABC):
    """
    A class to implement Twitter API v2
    Link docs: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/introduction

    Attributes
    -------------
    schema : dict
        schema of final output table to load to BigQuery.
        Since we load dataframe, the type should be align with pandas data type https://pbpython.com/pandas_dtypes.html
    dataset_name : str
        Name of the output dataset in Bigquery
    table_name : str
        Name of the output table in Bigquery

    Methods
    -------------
    load_to_dataframe
        load raw_data to Pandas dataframe, with schema provide above
    upload_data
        load data to BigQuery
    """

    access = None
    base_url = "https://api.twitter.com/2"
    raw_data = []

    service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson[
        "key_path"
    ]

    def __init__(self, account) -> None:
        super().__init__()
        self.account = account

    @property
    @abstractmethod
    def schema():
        pass

    @property
    @abstractmethod
    def dataset_name():
        pass

    @property
    @abstractmethod
    def table_name():
        pass

    @abstractmethod
    def get_full_bq_table_name():
        pass

    def load_to_dataframe(self):
        if isinstance(self.raw_data, dict):
            # pd.DataFrame automatically normalize data at different level that we can't control, so switch to pd.json_normalize
            df = pd.json_normalize(self.raw_data, max_level=0)
            # Keep columns in the right order due to lack of parameter in pd.json_normalize method
            df = df[self.schema.keys()]
        elif isinstance(self.raw_data, list):
            df = pd.DataFrame(self.raw_data, columns=self.schema.keys())

        # handle NaN in numeric column, because pandas can't convert NaN to numeric
        numeric_column = [k for k, v in self.schema.items() if v == "int64"]
        # Need to loop through each column because pandas doesn't allow to provide copy of slice from a DataFrame
        for column in numeric_column:
            df[column].fillna(0, inplace=True)

        df = df.astype(self.schema)
        return df

    def _prepare_before_upload(self, collected_ts):
        def call(df: pd.DataFrame):
            if df.empty:
                return df
        
            transformed_df = df[list(self.schema.keys())]
            
            # datetime_columns = [k for k, v in self.schema.items() if v == "datetime64[ns]"]
            # for column in datetime_columns:
            #     if pd.api.types.is_datetime64tz_dtype(transformed_df[column]): 
            #         transformed_df[column] = transformed_df[column].dt.tz_localize(None)

        
            # handle NaN in numeric column, because pandas can't convert NaN to numeric
            numeric_column = [k for k, v in self.schema.items() if v == "int64"]
            # Need to loop through each column because pandas doesn't allow to provide copy of slice from a DataFrame
            for column in numeric_column:
                transformed_df[column].fillna(0, inplace=True)

            transformed_df = transformed_df.astype(self.schema)

            transformed_df["__collected_ts"] = collected_ts
            transformed_df.info(memory_usage="deep")
            return transformed_df

        return call

    def initiate_uploader(self):
        dataset, table = self.get_full_bq_table_name().split(".")
        self.uploader = BigQueryDataUpload(
            self.service_account_json_path, dataset, table
        )

    def upload_data(self):
        self.initiate_uploader()
        df = self.load_to_dataframe()
        self.uploader.load_dataframe(df)
