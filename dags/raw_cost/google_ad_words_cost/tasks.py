from enum import Enum
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.decorators import task
from utils.constants import COUNTRY_CODE
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from .operator import GoogleAdWordsCostData


class GoogleAdWordsCostTaskGroup(TaskGroup):

    def __init__(
            self,
            group_id: str,
            bq_project: str,
            bq_dataset: str,
            gcs_bucket: str,
            gcp_conn_id: str,
            tooltip="Google Ad Words Cost Task Group",
            **kwargs
        ):

        super().__init__(
            group_id=group_id,
            tooltip=tooltip,
            **kwargs
        )

        self.bq_project  = bq_project
        self.bq_dataset  = bq_dataset
        self.gcs_bucket  = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

        self.gcs_prefix = 'costs/google_ad_words_cost'
        self.bq_table_name = 'raw_google_ad_words_cost'

        
        @task(task_group=self)
        def create_big_lake_table_raw_google_ad_words_cost_task():

            partition_expr = "{snapshot_date:DATE}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = self.bq_table_name,
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = self.gcs_prefix,
                gcs_partition_expr = partition_expr,
            )
        
        @task(task_group=self)
        def pull_google_ad_words_cost():
        
            return GoogleAdWordsCostData(
                gcs_bucket=self.gcs_bucket,
                gcs_prefix=self.gcs_prefix,
            ).execute()
        



        create_big_lake_table_raw_google_ad_words_cost_task() << [
            pull_google_ad_words_cost(

            )
        ]
