from max_mediation.hooks.max_reporting_api import MaxMediationReportingHook
import time
import datetime
import uuid
from typing import Any, Sequence
from airflow.macros import ds_format, ds_add

from airflow.models.baseoperator import BaseOperator
from max_mediation.hooks.max_reporting_api import MaxMediationReportingHook
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from max_mediation.scripts.max_utils import Params


class CohortOperator(BaseOperator, GCSDataUpload):
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
        endpoint,
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)

        self.hook = MaxMediationReportingHook(http_conn_id=http_conn_id)
        self.ds = ds
        self.endpoint = endpoint
        self.endpoint_to_columns = {
                '/maxCohort'        : Params.COHORT_AD_REVENUE_PERPORMANCE_COLUMNS,
                '/maxCohort/imp'    : Params.COHORT_AD_IMPRESSIONS_INFO_COLUMNS,
                '/maxCohort/session': Params.COHORT_SESSION_INFO_COLUMNS
            }   

    def execute(self, context: Any):
        data = self._get_data() 
        self._upload_data(data)

    def _get_data(self):
        return self.hook.get_cohort(
            self.ds, 
            self.endpoint, 
            self._format_required_columns(self.endpoint_to_columns.get(self.endpoint))
        )
    
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
    
    def _format_required_columns(self, columns):
        columns = [
                column.replace('X', required_number)
                    for column in columns
                        for required_number in Params.COHORT_REQUIRED_X_NUMBER
            ]
        return ",".join(list(dict.fromkeys(columns)))