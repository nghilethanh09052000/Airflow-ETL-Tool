from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow import DAG
from google.cloud import storage
from datetime import datetime, timedelta
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from play2airdrop.scripts.copy_and_convert import CopyAndConvertPlay2AirdropData



DAG_START_DATE = set_env_value(
    production=datetime(2024, 6, 24), dev=datetime(2024, 6, 12)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 11 * * *", dev="@once")

GCP_CONN_ID = "sipher_gcp"
BIGQUERY_PROJECT = "sipher-data-platform"
BQ_DATASET = "play2airdrop"
SOURCE_BUCKET = "aws-atherlabs-data"
DESTINATION_BUCKET = "aws-atherlabs-data-clean-csv"
OBJECT_PREFIX = 'play2airdrop'

@task(task_id="create_big_lake_table")
def create_big_lake_table_task(bq_table_name, gcs_prefix, gcs_partition_expr):
    return create_external_bq_table_to_gcs(
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        bq_table=bq_table_name,
        gcs_bucket=DESTINATION_BUCKET,
        gcs_object_prefix=gcs_prefix,
        gcs_partition_expr=gcs_partition_expr,
)
    
@task(task_id='delete_old_snapshots')
def delete_old_snapshots(
    gcs_bucket: str,
    gcs_prefix: str,
    **kwargs
):
    client = storage.Client.from_service_account_json(BaseHook.get_connection(GCP_CONN_ID).extra_dejson[ "key_path"])
    bucket = client.bucket(gcs_bucket)
    blobs = bucket.list_blobs(prefix=gcs_prefix)
    blobs_list = list(blobs)
    
    
    if len(blobs_list) > 1:  
        for blob in blobs_list:
            blob.delete()
        return f'Deleted {len(blobs_list)} old snapshots from {gcs_bucket}/{gcs_prefix}'
    else:
        return 'No snapshots to delete'

    
DATA = [
    {
        'file': 'Modifier.json',
        'name': 'modifier',
        'types': {
            'id': 'string',
            'title': 'string',
            'description': 'string',
            'baseId': 'string',
            'bonusPercentage': 'float64',
            'enabled': 'bool',
            'createdAt': 'string',
            'updatedAt': 'string'
        }
    },
    {
        'file': 'P2eFirstUsers.json',
        'name': 'p2e_first_users',
        'types': {
            'id': 'string',
            'userId': 'string',
            'createdAt': 'string'
        }
    },
    {
        'file': 'P2eQuest.json',
        'name': 'p2e_quest',
        'types': {
            'id': 'string',
            'title': 'string',
            'description': 'string',
            'longDescription': 'string',
            'basePoints': 'int64',
            'enabled': 'bool',
            'baseId': 'string',
            'predecessorQuestKey': 'string',
            'key': 'string',
            'createdAt': 'string',
            'updatedAt': 'string'
        }
    },
    {
        'file': 'P2eQuestMetadata.json',
        'name': 'p2e_quest_metadata',
        'types': {
            'id': 'string',
            'p2eQuestId': 'string',
            'key': 'string',
            'value': 'string',
            'createdAt': 'string'
        }
    },
    {
        'file': 'P2eSpecialUsers.json',
        'name': 'p2e_special_users',
        'types': {
            'id': 'string',
            'userId': 'string',
            'description': 'string',
            'refCode': 'string',
            'tier': 'int64',
            'type': 'string',
            'createdAt': 'string'
        }
    },
    {
        'file': 'P2eUserQuest.json',
        'name': 'p2e_user_quest',
        'types': {
            'id': 'string',
            'userId': 'string',
            'p2eQuestId': 'string',
            'amount': 'int64',
            'extraData': 'object',
            'signature': 'float64',
            'guarantee': 'string',
            'status': 'string',
            'createdAt': 'string',
            'updatedAt': 'string'
        }
    },
    {
        'file': 'P2eUserQuestClaimedLog.json',
        'name': 'p2e_user_quest_claimed_log',
        'types': {
            'id': 'string',
            'userId': 'string',
            'to': 'string',
            'extraData': 'object',
            'amount': 'int64',
            'p2eQuestId': 'int64',
            'description': 'string',
            'questName': 'string',
            'createdAt': 'string'
        }
    },
    # {
    #     'file': 'P2eUserQuestLog.json',
    #     'name': 'p2e_user_quest_log',
    #     'types': {}
    # }
]



default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
    "task_concurrency": 3,
    "concurrency": 3,
    "max_active_tasks": 3
}


with DAG(
    dag_id="play2airdrop",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["play2aidrop", "copy bucket", "convert data"],
    catchup=False
) as dag:
    partition_expr = "{dt:DATE}"
    
    for _ in DATA:
        
        
        copy_tasks = CopyAndConvertPlay2AirdropData(
            task_id=f"{_.get('name')}",
            gcp_conn_id=GCP_CONN_ID,
            src_bucket_name=SOURCE_BUCKET,
            des_bucket_name=DESTINATION_BUCKET,
            object=OBJECT_PREFIX,
            data=_,
            
        )
        create_big_lake_table_tasks = create_big_lake_table_task.override(task_id=f"create_big_lake_table_{_.get('name')}")(
            bq_table_name=f"{_.get('name')}",
            gcs_prefix=f"{OBJECT_PREFIX}/{_.get('name')}",
            gcs_partition_expr=partition_expr,
        )
        
        delete_old_snapshots_tasks = delete_old_snapshots.override(task_id = f"delete_old_snapshots_{_.get('name')}")(
            gcs_bucket=DESTINATION_BUCKET,
            gcs_prefix=f"{OBJECT_PREFIX}/{_.get('name')}/"
        )
        
        delete_old_snapshots_tasks >> copy_tasks >> create_big_lake_table_tasks