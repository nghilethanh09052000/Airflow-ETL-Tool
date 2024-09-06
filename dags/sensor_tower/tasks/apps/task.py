from airflow.utils.task_group import TaskGroup
from sensor_tower.operators import AppsEndpointOperator
from airflow.decorators import task

def task_get_us_apps_data_with_app_ids_bq(
    ds: str,
    task_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    http_conn_id: str
):
    return AppsEndpointOperator(
        ds=ds,
        task_id=task_id,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        http_conn_id=http_conn_id,
        os=['ios', 'android'],
        country='US'
    )