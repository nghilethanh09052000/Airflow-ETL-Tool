from typing import List, Dict
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from google.cloud import storage
from typing import BinaryIO
from google.cloud.storage.bucket import Bucket
import os
import re
import logging

import pandas as pd
import io
import json

class CopyAndConvertPlay2AirdropData(PythonOperator):
    def __init__(
        self, 
        task_id: str,
        gcp_conn_id: str, 
        src_bucket_name: str,
        des_bucket_name:str,
        object: str,
        data: List[Dict[str,str]],
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
        self.source_bucket: Bucket = self.storage_client.bucket(src_bucket_name)
        self.destination_bucket: Bucket = self.storage_client.bucket(des_bucket_name)
        self.data = data
        self.object = object
        self.blobs: BinaryIO = self.source_bucket.list_blobs(prefix=object)
    
    def _handle_extract_date_from_blob_name(self, blob):
        pattern = r"dt=(\d{4}-\d{2}-\d{2})-\d{2}-\d{2}-\d{2}"
        match = re.search(pattern, blob)
        if match:
            date_part = match.group(1)
            return date_part
    
    def _handle_extract_extraData_column(self, data):
        extra_data_columns = ['invite', 'playGame', 'reachLevel', 'times', 'totalBonusPercentage']
        new_columns = {column: [] for column in extra_data_columns}
        
        for index, row in data.iterrows():
            extra_data = row.get('extraData', {})
            if not extra_data:
                extra_data = {}
            for column in extra_data_columns:
                if column == 'totalBonusPercentage':
                    # Handle totalBonusPercentage separately as float
                    new_columns[column].append(float(extra_data.get(column, 0.0)))
                else:
                    value = extra_data.get(column, None)
                    new_columns[column].append(None if not value or value == [] else [str(v) for v in value])

        # Add the new columns to the dataframe
        for column, values in new_columns.items():
            data[column] = values
        
        if 'extraData' in data.columns:
            data.drop(columns=['extraData'], inplace=True)
        
        return data


    
    def execute_task(self, **kwargs):
        blobs = list(self.blobs)
        for blob in blobs:
            if blob.name.split('/')[-1] != self.data.get('file'): continue
            self._handle_process_blob(blob=blob)
            
    def _handle_process_blob(self, blob):
        
        date_from_blob = self._handle_extract_date_from_blob_name(blob.name)
        file_name = self.data \
                        .get('file') \
                        .split('.')[0]
        destination_folder = f"{self.object}/{self.data.get('name')}/dt={date_from_blob}"
        destination_blob_name = os.path.join(destination_folder, f"{file_name}.parquet")
        destination_blob = self.destination_bucket.blob(str(destination_blob_name))
        
        if not destination_blob.exists():
            self._handle_convert_json_to_parquet(
                    source_blob=blob,
                    destination_blob=destination_blob
                )
        else:
            logging.info(f'Snapshot Parquet Blob have already been existed, Skip Converting...{destination_blob}')
            return
    
    def _handle_convert_json_to_parquet(
            self, 
            source_blob, 
            destination_blob
        ):
        json_content = source_blob.download_as_text()

        if not json_content or json_content == '[]':
            logging.info(f"JSON file is empty, skipping conversion: {destination_blob}")
            return
        
        data = pd.read_json(io.StringIO(json_content))

        for column, dtype in self.data['types'].items():
            data[column] = data[column].astype(dtype)
            
        data = self._handle_extract_extraData_column(data=data)
        
        
        parquet_content = data.to_parquet(engine='pyarrow', index=False)
        destination_blob.upload_from_string(parquet_content)
        logging.info(f"Parquet content uploaded to------------------------{destination_blob}")

