from google.cloud import storage
from airflow.hooks.base import BaseHook

class GooglePlayHook():

    def __init__(
            self, 
            gcp_conn_id : str, 
            source_bucket : str,
            source_object : str,
            **kwargs
        ):
        self.gcp_conn_id = gcp_conn_id
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.gcsfs= storage.Client.from_service_account_json(
            BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"]
        )
        self.bucket = self.gcsfs.bucket(self.source_bucket)
    def list_blobs_name(self):
        list_blobs = list(self.gcsfs.list_blobs(self.source_bucket, prefix=self.source_object))
        return list_blobs

