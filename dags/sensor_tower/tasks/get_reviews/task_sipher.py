from typing import Dict, List, Sequence, Any,  Literal
from airflow.utils.task_group import TaskGroup
from sensor_tower.operators import GetReviewsEndpointOperator
from utils.constants import COUNTRY_CODE

"""
    Naming Convention:
    - Group tasks: group_tasks_get_**
    - Task: task_get_**
    **: Requirement
"""

"""
    app_ids type: Dict[str , Literal['android', 'ios']]:
    {
        'abc': 'android',
        '123': 'ios'
    }
"""


def task_sipher_get_review_us_country_on_specific_app_ids(
    ds: str,
    task_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    http_conn_id: str
):

    app_ids: Dict[str , Literal['android', 'ios']] = {
        'com.atherlabs.sipherodyssey.arpg':'android',
        '6443806584':'ios'
        }

    return GetReviewsEndpointOperator(
        ds=ds,
        task_id=task_id,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        http_conn_id=http_conn_id,
        os=['ios', 'android'],
        app_ids=app_ids,
        countries=['US'],
        rating_filter='',
        search_term='',
        username='',
        limit=200,
        page=1
    )
