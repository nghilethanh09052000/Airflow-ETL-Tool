from typing import List, Dict
import os
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from google.cloud import storage
import re
import time
import logging
from typing import BinaryIO
from google.cloud.storage.bucket import Bucket
import pandas as pd
import io

class CopyRawAdjustGcsOperator(PythonOperator):

    def __init__(
            self, 
            task_id: str,
            gcp_conn_id: str, 
            init_bucket_name: str,
            des_bucket_name:str,
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
        self.source_bucket: Bucket = self.storage_client.bucket(init_bucket_name)
        self.destination_bucket: Bucket = self.storage_client.bucket(des_bucket_name)
        self.blobs: BinaryIO = list(self.storage_client.list_blobs(init_bucket_name))
    
    def execute_task(self, **kwargs):

        for blob in self.blobs:
            self._handle_process_blob(blob=blob)

    def _handle_extract_date_from_blob_name(
                self, 
                blob_name: str
            ):
            """Extracts date from the blob name using a regular expression."""
            match = re.search(r'\d{4}-\d{2}-\d{2}', blob_name)
            return match.group() if match else None

    def _handle_process_blob(
            self,
            blob:BinaryIO
        ):
        """Process each blob and copy to the destination if it doesn't exist."""
        date_from_blob = self._handle_extract_date_from_blob_name(blob.name)
        if date_from_blob:

            destination_folder = f'data/snapshot={date_from_blob}'
            parquet_blob_name = os.path.splitext(os.path.basename(blob.name))[0][:-4]
            destination_blob_name = os.path.join(destination_folder, f"{parquet_blob_name}.parquet")
            destination_blob = self.destination_bucket.blob(str(destination_blob_name))
            
            if not destination_blob.exists():
                self._handle_convert_csv_to_parquet(
                        source_blob=blob,
                        destination_blob=destination_blob
                    )
            else:
                logging.info(f'Snapshot Parquet Blob have already been existed, Skip Converting...{destination_blob}')
                return
    

    def _handle_convert_csv_to_parquet(
            self, 
            source_blob, 
            destination_blob
        ):

        csv_content = source_blob.download_as_text()

        data = pd.read_csv(io.StringIO(csv_content), low_memory=False)
        data = data.astype(str)

        parquet_content = data.to_parquet(engine='pyarrow', index=False)


        """Upload Parquet content to GCS"""
        destination_blob.upload_from_string(parquet_content)

        logging.info(f"Parquet content uploaded to------------------------{destination_blob}")


                
                   