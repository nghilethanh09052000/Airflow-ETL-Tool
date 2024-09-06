import logging
from datetime import datetime, timedelta
import time

from airflow.models import Variable

from social.scripts.twitter.twitter_base import TwitterExtract, TwitterUpload
from utils.constants import PandasDtypes
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat

EXECUTION_ENVIRONMENT = Variable.get("execution_environment", default_var="dev")
BUCKET = Variable.get("ingestion_gcs_bucket", default_var="atherlabs-test")


class TwitterTimeline(TwitterExtract, TwitterUpload):

    schema = {
        "created_at": PandasDtypes.DATETIME,
        "id": PandasDtypes.INT64,
        "referenced_tweets": PandasDtypes.NESTED_ARRAY,
        "in_reply_to_user_id": PandasDtypes.INT64,
        "reply_settings": PandasDtypes.STRING,
        "text": PandasDtypes.STRING,
        "lang": PandasDtypes.STRING,
        "public_metrics": PandasDtypes.NESTED_ARRAY,
        "author_id": PandasDtypes.INT64,
        "conversation_id": PandasDtypes.INT64,
        "entities": PandasDtypes.NESTED_ARRAY,
        "possibly_sensitive": PandasDtypes.BOOL,
        "attachments": PandasDtypes.NESTED_ARRAY,
        "context_annotations": PandasDtypes.NESTED_ARRAY,
    }

    dataset_name = "raw_social" if EXECUTION_ENVIRONMENT == "production" else "tmp3"
    table_name = "twitter_timeline"
    gcs_prefix = "social_data/twitter/timeline"

    def __init__(
        self,
        account: str,
        start_date: str = None,
        end_date: str = None,
        day_diff: int = 30,
        **kwargs,
    ):
        super().__init__(account, **kwargs)
        self.access = account
        self.raw_data = []

        self.ds = kwargs.get("ds")
        self.start_date = start_date or datetime.strftime(
            datetime.strptime(self.ds, "%Y-%m-%d") - timedelta(days=day_diff),
            "%Y-%m-%d",
        )
        self.end_date = end_date or self.ds

    def create_url(self):
        return f"{self.base_url}/users/{self.access['user_id']}/tweets"

    def get_params(self, pagination_token=None):
        return {
            "tweet.fields": "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld",
            "start_time": f"{self.start_date}T00:00:00Z",
            "end_time": f"{self.end_date}T00:00:00Z",
            "max_results": 100,
            "pagination_token": pagination_token,
        }

    def pull_data(self):
        self.initiate_extractor("oauth1")

        while True:
            r = self.extractor.make_request()
            self.raw_json = r.json()
            if "data" in self.raw_json.keys():
                self.raw_data += self.raw_json["data"]
            logging.info(f"results: {self.raw_data})")

            pagination_token = self.raw_json.get("meta").get("next_token")
            self.extractor.params = self.get_params(pagination_token)
            if pagination_token is None:
                break
        logging.info("Pulling tweet timeline data: done")

    def get_full_bq_table_name(self):
        return f"{self.dataset_name}.{self.table_name}__{self.access.get('user_name')}"

    def upload_data(self):
        self._upload_to_gcs()

    def _upload_to_gcs(self):
        import uuid

        partition_prefix = (
            f"account={self.access.get('user_id')}/snapshot_date={self.ds}"
        )
        collected_ts = round(time.time() * 1000)
        gcs = GCSDataUpload(BUCKET, self.gcs_prefix)
        gcs.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=self.raw_data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
