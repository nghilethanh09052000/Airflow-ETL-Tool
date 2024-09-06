from enum import Enum
from airflow.operators.python import PythonOperator
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from airflow.hooks.base import BaseHook
from google.cloud import compute_v1
from google.oauth2 import service_account

class GetBSCScanTransationsToken(
    PythonOperator, 
    GCSDataUpload
):

    def __init__(
        self, 
        task_id: str,
        gcp_conn_id: str, 
        **kwargs
    ):
        PythonOperator.__init__(
            self, 
            task_id=task_id, 
            provide_context=True, 
            python_callable=self.execute_task, 
            **kwargs
        )

        self.gcp_conn_id = gcp_conn_id
    
    def execute_task(self):
        credentials = service_account.Credentials.from_service_account_file(
            BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"]
        )

        networks_client = compute_v1.NetworksClient(credentials=credentials)
        for network in networks_client.list(project='YOUR_PROJECT'):
            print(network)

       