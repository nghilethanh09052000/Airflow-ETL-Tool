import os
import gc
from google.oauth2 import service_account
from google.cloud import storage
from airflow.hooks.base import BaseHook

import logging
from abc import ABC, abstractmethod

class AWSFileGCS(ABC):
    '''
    The class to process raw files in GCS that are initially transfered from AWS

    Methods
    -------------
    get_object_list
        return the list gcs paths and blobs for the files needed 
    download_and_get_object_local_path
        return the local file paths of downloaded raw files
    '''

    def __init__(self, ds):
        self.service_account_json_path = BaseHook.get_connection('sipher_gcp').extra_dejson['key_path']
        self.project = 'sipher-data-platform'
        self.input_bucket_name = 'aws-atherlabs-data'
        self.date = ds


    @abstractmethod
    def run(self):
        pass


    def init_gcs_client(self):
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_json_path)
        self.storage_client = storage.Client(credentials=self.credentials, project=self.project)
        self.input_bucket = self.storage_client.get_bucket(bucket_or_name=self.input_bucket_name)

        self.output_bucket_name = 'aws-atherlabs-data-clean-csv'
        self.output_bucket = self.storage_client.get_bucket(bucket_or_name=self.output_bucket_name)


    def get_object_list(self, prefix_path):
        """Lists all the blobs in the bucket with path prefix."""
        blobs = self.storage_client.list_blobs(bucket_or_name = self.input_bucket_name, prefix = prefix_path)
        object_list = []
        for blob in blobs:
            object_list.append(blob.name)
        return object_list
    
    
    def download_and_get_object_local_path(self, object_name, object_file_type, blob):        
        try:
            cwd = os.getcwd()
            path = os.path.join(cwd, 'dags/tmp')
            if not os.path.exists(path):
                os.makedirs(path)
            local_file_name = object_name
            file_path = os.path.join(path, local_file_name)
            os.makedirs(path, exist_ok=True)
            with open(file_path + object_file_type, 'wb') as f:
                self.storage_client.download_blob_to_file(blob, f)
            return file_path
        
        except Exception as e:
            logging.error(e)
            return False
    
    
    @abstractmethod
    def find_object_path_derived(self, object_path, type=None):
        pass
    
    
    @abstractmethod
    def clean_file_content_to_parquet(self, **args):
        pass


    def upload_file_to_gcs(self, object_type, object_name, object_file_type, file_path):
        logging.info('.....START UPLOADING.....')
        try:
            blob = self.output_bucket.blob(blob_name = f'{self.output_folder_name}-{object_type}/dt={self.date}/{object_name}.parquet')
            blob.upload_from_filename(filename= file_path + '.parquet', timeout=180)
            blob.update_storage_class("STANDARD")

            os.remove(file_path + '.parquet')
            os.remove(file_path + object_file_type)
            gc.collect()
            
            
            return True, logging.info(f'Uploaded blob: {self.output_folder_name}-{object_type}/dt={self.date}/{object_name}.parquet')
        
        except Exception as e:
            logging.error(e)
                        
            os.remove(file_path + '.parquet')
            os.remove(file_path + object_file_type)
            gc.collect()
            
            raise

    
    @classmethod
    def airflow_callable(cls, ds):
        ins = cls(ds)
        ins.run()