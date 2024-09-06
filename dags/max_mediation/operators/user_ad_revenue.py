import time
import json
import jinja2
import uuid
import logging
from typing import Dict, Callable, Union, List, Any
import pandas as pd
from typing import Any, Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from max_mediation.hooks.max_reporting_api import MaxMediationReportingHook
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from max_mediation.scripts.max_utils import Params


class MaxUserAdRevenueOperator(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )

    def __init__(
        self,
        http_conn_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        gcp_conn_id: str,
        bq_project: str,
        ds: str,
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)
        self.bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

        self.hook = MaxMediationReportingHook(http_conn_id=http_conn_id)

        self.ds = ds
        self.bq_project = bq_project
        self.query = Params.APP_META_QUERY
        self.query_job_config = Params.QUERY_JOB_CONFIG

    def execute(self, context: Any):
        data = self._get_data() 
        self._upload_data(data)

    def _get_data(self):
        user_ad_revenue_df = pd.DataFrame()
        app_meta_query_job_result = self.query_app_meta_data()

        for item in app_meta_query_job_result:
            package_name, platform = item['package_name'], item['platform']
            user_ad_revenue_data = self.hook.get_user_ad_revenue(self.ds, package_name, platform)
            tmp_df = pd.DataFrame(user_ad_revenue_data)
            tmp_df['app_id'] = package_name
            tmp_df['platform'] = platform
            user_ad_revenue_df = pd.concat([user_ad_revenue_df, tmp_df], ignore_index=True)

        return user_ad_revenue_df

    def _upload_data(self, data):
        collected_ts = round(time.time() * 1000)
        partition_prefix = f"snapshot_date={self.ds}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
    
    def query_app_meta_data(self):
        environment = jinja2.Environment()
        query = environment.from_string(self.query).render(ds=self.ds, bq_project=self.bq_project)
        job_config = json.loads(environment.from_string(self.query_job_config).render(query=query))
        job_config['query']['use_legacy_sql'] = False

        job = self.bq_hook.insert_job(configuration=job_config)
        return job.result()
