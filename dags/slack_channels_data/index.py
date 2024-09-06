from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from utils.common import set_env_value
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from utils.alerting.airflow import airflow_callback
from datetime import datetime, timedelta
from slack_channels_data.scripts import (
    ListAllMessages, 
    ListUsers, 
    ListChannels, 
    ListPermanentLinks
)



DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 2), 
    dev=datetime(2024, 5, 16)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 7,19 * * *", dev="@daily")
GCP_CONN_ID = "sipher_gcp"
GCP_PREFIX = 'slack_data'
BUCKET = 'atherlabs-ingestion'
#BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
#BIGQUERY_PROJECT = Variable.get("bigquery_project")
BIGQUERY_PROJECT = 'sipher-data-platform'

BQ_DATASET = 'slack_data'
CHANNEL_IDS = Variable.get('atherlabs_slack_channel_ids').split(',')
SLACK_TOKEN = Variable.get('slack_sipher_odyssey_alerting_token')

@task(task_id="create_big_lake_table")
def create_big_lake_table_task(bq_table_name, gcs_prefix, gcs_partition_expr):
    return create_external_bq_table_to_gcs(
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        bq_table=bq_table_name,
        gcs_bucket=BUCKET,
        gcs_object_prefix=gcs_prefix,
        gcs_partition_expr=gcs_partition_expr,
    )



default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "one_success",
    "retries": 8,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_callback,
}


with DAG(
    dag_id="slack_channels_data",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["Slack", "Channel", "Message Data"],
    catchup=False
) as dag:
    
    partition_expr = "{snapshot_date:DATE}"

    list_users = ListUsers(
        task_id='list_users',
        team_id='',
        token=SLACK_TOKEN,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCP_PREFIX}/slack_users",
        ds = "{{ ds }}"  
    )

    create_big_lake_table_slack_users = create_big_lake_table_task.override(task_id="create_big_lake_table_slack_users")(
        bq_table_name=f"slack_users",
        gcs_prefix=f"{GCP_PREFIX}/slack_users",
        gcs_partition_expr=partition_expr,
    )

    list_channels = ListChannels(
        task_id='list_channels',
        token=SLACK_TOKEN,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCP_PREFIX}/slack_channels",
        ds = "{{ ds }}"  
    )

    create_big_lake_table_slack_channels = create_big_lake_table_task.override(task_id="create_big_lake_table_slack_channels")(
        bq_table_name=f"slack_channels",
        gcs_prefix=f"{GCP_PREFIX}/slack_channels",
        gcs_partition_expr=partition_expr,
    )

    list_all_messages = ListAllMessages(
        task_id='list_all_messages',
        channel_ids=CHANNEL_IDS,
        token=SLACK_TOKEN,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCP_PREFIX}/slack_messages",
        project_id=BIGQUERY_PROJECT,
        gcp_conn_id=GCP_CONN_ID,
        ds = "{{ ds }}"  
    )

    create_big_lake_table_slack_messages = create_big_lake_table_task.override(task_id="create_big_lake_table_slack_messages")(
        bq_table_name=f"slack_messages",
        gcs_prefix=f"{GCP_PREFIX}/slack_messages",
        gcs_partition_expr=partition_expr,
    )

    list_permanent_links = ListPermanentLinks(
        task_id='list_permanent_links',
        token=SLACK_TOKEN,
        gcs_bucket=BUCKET,
        gcs_prefix=f"{GCP_PREFIX}/slack_permanent_links",
        bq_project=BIGQUERY_PROJECT,
        gcp_conn_id=GCP_CONN_ID,
        ds = "{{ ds }}",
    )

    create_big_lake_table_permanent_links = create_big_lake_table_task.override(task_id="create_big_lake_table_permanent_links")(
        bq_table_name=f"slack_permanent_links",
        gcs_prefix=f"{GCP_PREFIX}/slack_permanent_links",
        gcs_partition_expr=partition_expr,
    )


    list_users >> create_big_lake_table_slack_users >> list_all_messages >> create_big_lake_table_slack_messages >> list_permanent_links >> create_big_lake_table_permanent_links

    list_channels >> create_big_lake_table_slack_channels 


    

   

   

  
