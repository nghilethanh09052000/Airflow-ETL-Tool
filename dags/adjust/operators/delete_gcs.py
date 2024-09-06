from airflow.utils.context import Context
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from google.cloud import storage
import re
import time
import logging
from typing import BinaryIO
from google.cloud.storage.bucket import Bucket
from datetime import datetime, timedelta




class DeleteRawAdjustGcsOperator(PythonOperator):

    def __init__(
        self, 
        task_id: str,
        gcp_conn_id: str, 
        bucket_name: str,
        specified_day: int,
        ds:str,
        **kwargs
    ):

        super().__init__(
            task_id=task_id, 
            provide_context=True, 
            python_callable=self.execute_task, 
            **kwargs
        )

        self.gcp_conn_id = gcp_conn_id
        self.storage_client = storage.Client.from_service_account_json(
            BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"]
        )
        self.source_bucket: Bucket = self.storage_client.bucket(bucket_name)
        self.blobs: BinaryIO = list(self.storage_client.list_blobs(bucket_name))

        self.specified_day=specified_day

    def execute_task(self, ds, **kwargs):
        self.ds = ds
        for blob in self.blobs:
            self._handle_delete_blobs(blob=blob)

    def _handle_delete_blobs(
            self,
            blob: BinaryIO
        ):

        date_from_blob = self._handle_extract_date_from_blob_name(blob.name)

        if self._handle_check_required_delete_date(
            init_date=self.ds,
            blob_date=date_from_blob,
            specified_date=self.specified_day
        ):
            blob.delete()
            logging.info(f'Blob Deleted: {blob.name} In Date Dag Run: {self.ds}')
        else:
            logging.info(f'Blob: {blob.name} is still in range of {self.specified_day} days in Date Dag Run {self.ds}. Nothing to delete')

    def _handle_extract_date_from_blob_name(
            self, 
            blob_name: str
        ) -> str:
        """Extracts date from the blob name using a regular expression."""
        match = re.search(r'\d{4}-\d{2}-\d{2}', blob_name)
        return match.group() if match else None
    
    def _handle_check_required_delete_date(
            self,
            init_date: str,
            blob_date: str,
            specified_date: int
        ) -> bool:
        
        transform_required_date = datetime.strptime(init_date, "%Y-%m-%d") -  timedelta(days=specified_date)
        return blob_date <= datetime.strftime(transform_required_date, "%Y-%m-%d")  
    
    
  
   