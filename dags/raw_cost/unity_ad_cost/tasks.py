from enum import Enum
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from utils.constants import COUNTRY_CODE
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from .operator import (
    UnityAdCostParams,
    UnityAdCostData
)
from .utils import (
    Scale,
    SplitBy,
    Fields,
    AdTypes,
    AppStores,
    Platforms
)

class UnityAdCostTaskGroup(TaskGroup):

    def __init__(
        self,
        group_id: str,
        bq_project: str,
        bq_dataset: str,
        gcs_bucket: str,
        gcp_conn_id: str,
        tooltip: str = "Unity Ad Cost Tasks Group",
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

        self.gcs_prefix = 'costs/unity_ad_cost'
        self.bq_table_name = 'raw_unity_cost'


        def _handle_return_comma_string(EnumClass: Enum)-> str:
            return ','.join(list(item.value for item in EnumClass))

        @task(task_group=self)
        def create_big_lake_table_raw_unity_cost_task():

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
        def pull_unity_ad_cost(
            start_date: str,
            end_date: str
        ):

            params = UnityAdCostParams(
                start=start_date,
                end=end_date,
                scale=Scale.ALL.value,
                splitBy=_handle_return_comma_string(SplitBy),
                fields='',
                campaignSets='',
                campaigns='',
                targets='',
                adTypes=_handle_return_comma_string(AdTypes),
                countries=','.join(COUNTRY_CODE.keys()),
                stores=_handle_return_comma_string(AppStores),
                platforms=_handle_return_comma_string(Platforms),
                osVersions='',
                creativePacks='',
                sourceAppIds='',
                skadConversionValues=''
            ).__dict__  


            return UnityAdCostData(
                gcs_bucket=self.gcs_bucket,
                gcs_prefix=self.gcs_prefix,
                params=params
            ).execute()
        
    
        create_big_lake_table_raw_unity_cost_task() << [

            pull_unity_ad_cost(
                start_date="{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
                end_date="{{ ds }}"
            )
        ] 
        
       
