from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from .script import GetListCoins
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs


class TaskGroupGetListCoins(TaskGroup):
     def __init__(
        self,
        group_id: str,
        bq_project: str,
        bq_dataset: str,
        gcs_bucket: str,
        gcs_prefix: str,
        gcp_conn_id: str,
        tooltip: str = "Get List Coins From Coin MarketCap",
        **kwargs
    ):
        super().__init__(
            group_id=group_id,
            tooltip=tooltip,
            **kwargs
        )
    
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.gcp_conn_id = gcp_conn_id

        self.bq_table_name = 'raw_coinmarketcap_list_coins'


        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_list_coins_task():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

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
        def get_all_coins(
            ds: str,
            **kwargs
        ):
            ti = kwargs['ti']
            execution_date = kwargs['execution_date']
            return GetListCoins(
                gcs_bucket = self.gcs_bucket,
                gcs_prefix= self.gcs_prefix,
                ds=ds,
                ti = ti,
                timestamp=int(execution_date.timestamp())
            ).run()
        

        get_all_coins(
            ds ="{{ ds }}"
        ) >> create_big_lake_table_raw_coinmarketcap_list_coins_task()