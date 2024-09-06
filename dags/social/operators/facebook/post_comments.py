import time
import uuid
from typing import Any, List, Sequence

from airflow.models.baseoperator import BaseOperator

from social.hooks.facebook.page_post import FacebookPagePostHook
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat


class FacebookPostCommentsOperator(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "post_ids",
        "bucket",
        "gcs_prefix",
    )

    ui_color = "#3b5998"

    def __init__(
        self,
        http_conn_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        post_ids: List[str],
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)

        self.post_ids = post_ids
        self.hook = FacebookPagePostHook(http_conn_id=http_conn_id)

    def execute(self, context: Any):
        ds = context["ds"]
        data = self._get_data()
        self._upload_data(data, ds)

    def _get_data(self):
        return self.hook.get_post_comments(post_ids=self.post_ids)

    def _upload_data(self, data, ds):
        partition_prefix = f"snapshot_date={ds}"
        collected_ts = round(time.time() * 1000)

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
