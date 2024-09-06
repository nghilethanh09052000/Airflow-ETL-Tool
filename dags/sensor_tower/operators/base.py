from typing import Any, Sequence
from airflow.utils.context import Context
from abc import abstractmethod
import time
import uuid
from typing import List
from airflow.models.baseoperator import BaseOperator
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat

class SensorTowerBaseOperator(BaseOperator, GCSDataUpload):

    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds"
    )
    
    def __init__ (
        self,
        ds,
        task_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        **kwargs
    ):
        
        BaseOperator.__init__(
            self, 
            task_id = task_id,
            **kwargs
        )

        GCSDataUpload.__init__(
            self, 
            gcs_bucket = gcs_bucket,
            gcs_prefix = gcs_prefix, 
            **kwargs
        )
        self.ds = ds

    @abstractmethod
    def execute(self, context: Context):
        pass

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
    