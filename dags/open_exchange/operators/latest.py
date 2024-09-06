import time
import uuid
from typing import Any, Sequence

from airflow.models.baseoperator import BaseOperator
from open_exchange.hooks.rates import OpenExchangeRatesHook
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat


class OpenExchangeLatestRateOperator(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
    )

    def __init__(
        self,
        http_conn_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        symbols: str = None,
        base: str = "USD",
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)

        self.base = base
        self.symbols = symbols
        self.hook = OpenExchangeRatesHook(http_conn_id=http_conn_id)

    def execute(self, context: Any):
        data = self._get_data()
        self._upload_data(data)

    def _get_data(self):
        return self.hook.get_latest(base=self.base, symbols=self.symbols)

    def _upload_data(self, data):
        collected_ts = round(time.time() * 1000)
        current_date = time.strftime("%Y-%m-%d")
        current_hour = time.strftime("%H")
        partition_prefix = f"snapshot_date={current_date}/snapshot_hour={current_hour}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
