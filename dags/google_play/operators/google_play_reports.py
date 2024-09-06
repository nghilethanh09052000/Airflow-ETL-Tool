from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
from google_play.hooks.google_play_hooks import GooglePlayHook
# from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from typing import List, Sequence, Union
import pandas as pd
from typing import List, Sequence
from zipfile import ZipFile
import re
import io
import pyarrow
import logging

class GCSFileTransformOperator(PythonOperator):
    """
    Copies data from a source GCS location to a temporary location on the local filesystem.

    Runs a transformation on this file as specified by the transformation script
    and uploads the output to a destination bucket. If the output bucket is not
    specified the original file will be overwritten.

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file.

    :param source_bucket: The bucket to locate the source_object. (templated)
    :param source_object: The key to be retrieved from GCS. (templated)
    :param destination_bucket: The bucket to upload the key after transformation.
        If not provided, source_bucket will be used. (templated)
    :param destination_object: The key to be written in GCS.
        If not provided, source_object will be used. (templated)
    :param transform_script: location of the executable transformation script or list of arguments
        passed to subprocess ex. `['python', 'script.py', 10]`. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "source_bucket",
        "source_object",
        "destination_bucket",
        "destination_object",
        "gcp_conn_id",
        "ds",
        # "transform_script",
        # "impersonation_chain",
    )


    def __init__(
        self,
        task_id:       str,
        source_bucket: str,
        source_object: str,
        gcp_conn_id:   str,
        # transform_script: str or List[str],
        destination_bucket: Union[str, None] = None,  # Fix the type hint here
        destination_object: Union[str, None] = None,  # Fix the type hint here
        ds: Union[str, None] = None,
        # impersonation_chain: str or Sequence[str] or None = None,
        **kwargs,
    ) -> None:

        super().__init__(
            task_id=task_id, 
            provide_context=True, 
            python_callable=self.execute, 
            **kwargs
        )

        self.ds = ds
        self.gcp_conn_id   = gcp_conn_id
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.hook_src = GooglePlayHook(gcp_conn_id=gcp_conn_id, source_bucket=source_bucket, source_object=source_object)
        self.destination_bucket = destination_bucket or self.source_bucket
        self.destination_object = destination_object or self.source_object
        self.hook_des = GooglePlayHook(gcp_conn_id=gcp_conn_id, source_bucket=destination_bucket,source_object=destination_object)
        # self.transform_script = transform_script
        # self.impersonation_chain = impersonation_chain
        
    def execute(self, **kwargs):
        # Read data from GCS
        src_bucket = self.hook_src.bucket
        src_list_blobs = self.hook_src.list_blobs_name()
        for blob in src_list_blobs:
            self._transform_and_upload(bucket=src_bucket,blob=blob)

    def _transform_and_upload(self, bucket, blob):
        extract_date_blob_name = self._extract_date_from_blob_name(blob_name=blob.name)
        logging.info(f"Check report of {extract_date_blob_name}: {blob.name}")
        if extract_date_blob_name == self.ds :
            src_blob = bucket.blob(str(blob.name))
            data = src_blob.download_as_string()
            logging.info(f"Retrieved data successfull from {bucket.name}")
            if blob.name.endswith(".zip"):
                # Perform zipped data transformation
                parquet_data = self._zip_to_parquet_file(data)
                # Write transformed data back to GCS
                self._upload_back_to_gcs(parquet_data, blob)
            else:
                # Perform csv data transformation
                parquet_data = self._csv_to_parquet_file(data)
                # Write transformed data back to GCS
                self._upload_back_to_gcs(parquet_data, blob)

    def _extract_date_from_blob_name(self, blob_name:str):
        extract_date = re.search(r'_(\d{6})', blob_name)
        return extract_date.group(1) if extract_date else None

    def _zip_to_parquet_file(self,data_name):
        in_memory_zip = io.BytesIO(data_name)
        with ZipFile(in_memory_zip, "r") as zip_ref:
            unzipped_content = zip_ref.read(zip_ref.namelist()[0]).decode("utf-8")
        data = pd.read_csv(io.StringIO(unzipped_content), low_memory=False)
        data = data.astype(str)
        parquet_content = data.to_parquet(engine='pyarrow', index=False)
        return parquet_content
    
    def _csv_to_parquet_file(self, data_name):
        csv_content = data_name.decode("utf-16")
        data = pd.read_csv(io.StringIO(csv_content), low_memory=False)
        data = data.astype(str)
        parquet_content = data.to_parquet(engine='pyarrow', index=False)
        return parquet_content
    
    def _upload_back_to_gcs(self, parquet_data, blob):
        partition_prefix = f"snapshot_year_month={self.ds}"
        report_info = blob.name.split("/")[-1][:-4]
        des_bucket = self.hook_des.bucket
        des_blob = des_bucket.blob(f"{self.destination_object}/{partition_prefix}/{report_info}.parquet")
        des_blob.upload_from_string(parquet_data)
        logging.info(f"Uploaded data successfull to {self.destination_object}/{partition_prefix}/")
