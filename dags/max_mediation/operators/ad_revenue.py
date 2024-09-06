from max_mediation.hooks.max_reporting_api import MaxMediationReportingHook
import time
import datetime
import uuid
from typing import Any, Sequence
from airflow.macros import ds_format, ds_add

from airflow.models.baseoperator import BaseOperator
from max_mediation.hooks.max_reporting_api import MaxMediationReportingHook
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat


class MaxAdRevenueOperator(BaseOperator, GCSDataUpload):
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
        ds,
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)

        self.hook = MaxMediationReportingHook(http_conn_id=http_conn_id)
        self.ds = ds


    def execute(self, context: Any):
        data = self._get_data() 
        self._upload_data(data)

    def _get_data(self):
        return self.hook.get_ad_revenue(self.ds)

    def _upload_data(self, data):
        collected_ts = round(time.time() * 1000)
        current_date = time.strftime("%Y-%m-%d")
        current_hour = time.strftime("%H")
        partition_prefix = f"snapshot_date={self.ds}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
