import logging
from datetime import datetime
import time
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from social.scripts.twitter.twitter_base import TwitterExtract, TwitterUpload
from utils.constants import PandasDtypes

from airflow.models import Variable



EXECUTION_ENVIRONMENT = Variable.get("execution_environment", default_var="dev")
BUCKET = Variable.get("ingestion_gcs_bucket", default_var="atherlabs-test")


class TwitterUserLookup(TwitterExtract, TwitterUpload):

    schema = {
        "created_at": PandasDtypes.DATETIME,
        "username": PandasDtypes.STRING,
        "id": PandasDtypes.INT64,
        "name": PandasDtypes.STRING,
        "public_metrics": PandasDtypes.NESTED_ARRAY,
        "verified": PandasDtypes.BOOL,
        "location": PandasDtypes.STRING,
        "description": PandasDtypes.STRING,
        "url": PandasDtypes.STRING,
        "protected": PandasDtypes.BOOL,
        "pinned_tweet_id": PandasDtypes.INT64,
        "profile_image_url": PandasDtypes.STRING,
        "entities": PandasDtypes.NESTED_ARRAY,
    }

    dataset_name = "raw_social" if EXECUTION_ENVIRONMENT == "production" else "tmp3"
    table_name = "twitter_profile_stats"
    gcs_prefix = "social_data/twitter/user"

    def __init__(
            self,
            account, 
            **kwargs
        ):
        super().__init__(account, **kwargs)
        self.access = account
        self.raw_data = []
        self.ds = kwargs.get("ds")

    def create_url(self):
        url = "https://api.twitter.com/2/users/me"
        return url

    def get_params(self):
        return {
            "user.fields": "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
        }

    def pull_data(self):
        self.initiate_extractor("oauth1")
        r = self.extractor.make_request()
        if "data" in r.json():
            self.raw_data = r.json()["data"]
        else:
            raise ValueError(f"There is no data. Check response body {r.json()}")

        logging.info(f"Pulling user info data: done, {self.raw_data}")

    def get_full_bq_table_name(self):
        date_suffix = datetime.strftime(datetime.today(), "%Y%m%d")
        return f"{self.dataset_name}.{self.table_name}__{self.account}__{date_suffix}"
    
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
