from airflow.utils.task_group import TaskGroup
from sensor_tower.operators import SalesReportEstimatesCompareAttrEndpointOperator


"""
    Naming Convention:
    - Group tasks: group_tasks_get_**
    - Task: task_get_**
    **: Requirement
"""

def task_get_sales_report_estimates_comparision_attributes_on_specific_os_and_categories(
    ds: str,
    task_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    http_conn_id: str
):
    for i in range(0, 11000, 1000):
        SalesReportEstimatesCompareAttrEndpointOperator(
            ds=ds,
            task_id=f"{task_id}_{i}",
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            http_conn_id=http_conn_id,
            os=['ios', 'android'],
            comparison_attribute='absolute',
            time_range='day',
            measure='units',
            device_type={'ios': 'total', 'android': ''},
            categories=[6014, 'game'],
            country='US',
            limit=1000,
            offset=int(i),
            custom_fields_filter_id='',
            custom_tags_mode=''
        )

def task_get_sales_report_estimates_comparision_attributes_on_unified_os_and_categories(
    ds: str,
    task_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    http_conn_id: str
):
    for i in range(0, 11000, 1000):
        SalesReportEstimatesCompareAttrEndpointOperator(
            ds=ds,
            task_id=f"{task_id}_{i}",
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            http_conn_id=http_conn_id,
            os=['unified'],
            comparison_attribute='absolute',
            time_range='day',
            measure='revenue',
            device_type={'ios': 'total', 'android': ''},
            categories=[ 7001, 7014, 7017 ],
            country='US',
            limit=1000,
            offset=int(i),
            custom_fields_filter_id='',
            custom_tags_mode=''
        )



