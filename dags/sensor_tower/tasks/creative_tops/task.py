from airflow.utils.task_group import TaskGroup
from sensor_tower.operators import CreativesTopsEndpointOperator

"""
    Naming Convention:
    - Group tasks: group_tasks_get_**
    - Task: task_get_**
    **: Requirement
"""

NETWORKS = [
    "Adcolony",
    "Admob",
    "Applovin",
    # "BidMachine",
    "Chartboost",
    # "Digital_Turbine",
    #"Facebook",
    #"InMobi",
    #"Instagram",
   #"Meta_Audience_Network",
   # "Mintegral",
    "Mopub",
    #"Pinterest",
    #"Smaato",
    "Snapchat",
    "Supersonic",
    #"Tapjoy",
    "TikTok",
    #"Twitter",
    "Unity",
    #"Verve",
    "Vungle",
    #ß"Youtube"
]


AD_TYPES = [
    'banner',
    'full_screen',
    'video',
    'playable'
]

def group_tasks_get_creative_tops_on_specific_os_countries_and_categories(
    ds: str,
    group_task_id,
    gcs_bucket,
    gcs_prefix,
    http_conn_id
):
    with TaskGroup(
        group_task_id,
        tooltip="Get Countries US, JP, CN, KR In specific categories based on requirements"
    ) as group_task_id:
        
        tasks = []
        for country in ['US', 'JP', 'CN', 'KR']:
            task = CreativesTopsEndpointOperator(
                    ds=ds,
                    task_id=f'get_creative_tops_apps_in_country_{country}',
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                    http_conn_id=http_conn_id,
                    os=['ios', 'android'],
                    period='week',
                    categories=['game', 6014],
                    country=country,
                    networks=NETWORKS,
                    ad_types=AD_TYPES,
                    limit=250,
                    page='',
                    placement=''
            )
            tasks.append(task)

    return group_task_id