from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
import requests
import json
import pandas as pd
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from google.cloud import bigquery
from datetime import datetime, timedelta
import time

BUCKET = Variable.get("ingestion_gcs_bucket", default_var="atherlabs-test")

class SensortowerTopApps():

    def __init__(self):
        self.service_account_json_path = BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
        self.gcs_prefix = "sensortower/top_apps"
        self.os_config = {'ios': {'category': 6014, 'device_type': 'total'}, 'android': {'category': 'game', 'device_type': ''}}

    @classmethod
    def airflow_callable(cls, ds):
        ins = cls()
        ins.upload_to_gcs(ds)

    def prepare_before_upload(self, collected_ts):
        def call(df: pd.DataFrame):
            if df.empty:
                return df
            transformed_df = df.copy()

            transformed_df["__collected_ts"] = collected_ts
            print(transformed_df.dtypes)
            transformed_df = transformed_df.astype(str)
            print(transformed_df.dtypes)
            transformed_df.info(memory_usage="deep")
            return transformed_df

        return call

    def upload_to_gcs(self, ds):
        print("EXEC_DATE: ", ds)
        for os in self.os_config:
            data = self.get_data(ds, os)
            partition_prefix = f"os={os}/snapshot_date={ds}"
            collected_ts = round(time.time() * 1000)
            gcs = GCSDataUpload(BUCKET, self.gcs_prefix)

            gcs.upload(
                object_name=f"/{partition_prefix}/top_apps_today",
                data=data,
                gcs_file_format=SupportedGcsFileFormat.PARQUET,
                pre_upload_callable=self.prepare_before_upload(collected_ts),
            )

    def get_data(self, ds, os):
        ROW_LIMIT = 1000

        URL = f'https://api.sensortower.com/v1/{os}/sales_report_estimates_comparison_attributes?comparison_attribute=absolute&time_range=day&measure=units&device_type={self.os_config[os]["device_type"]}&category={self.os_config[os]["category"]}&date={ds}&end_date={ds}&country=US&limit={ROW_LIMIT}&custom_tags_mode=include_unified_apps&auth_token=ST0_IxfWtBxS_bQy1nsGq7GL2Ee'
        print('-----',URL)
        # Make API request
        response = requests.get(URL)
        data = json.loads(response.content)
        print(len(data))

        return data

    

