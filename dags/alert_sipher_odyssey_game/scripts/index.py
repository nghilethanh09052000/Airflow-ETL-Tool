from airflow.decorators import task
from google.cloud import bigquery
from slack_sdk import WebClient
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
from collections import defaultdict
import logging


slack_token = Variable.get('slack_sipher_odyssey_alerting_token')
client = WebClient(token=slack_token)
dashboard_url = 'https://lookerstudio.google.com/reporting/aa5b3982-adbf-4917-ba0b-dc2608269fa9/page/BP0mD'
users = ', '.join([ f"@{name}" for name in Variable.get('sipher_odyssey_alerting_user_abnormal').split(', ')])
channel = "C06TMF0CSF3" 


def _get_data(
    ds,
    gcp_conn_id,
    bigquery_project
):
    previous_date = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
    formatted_previous_date = previous_date.strftime("%Y%m%d")


    query = f"""
        WITH 
            raw AS
            (
                SELECT 
                    event_timestamp,
                    user_id,
                    PARSE_DATE("%Y%m%d", event_date) AS event_date,
                    (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='trace_id') AS trace_id,
                    (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='item_type') AS item_type,
                    (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='type') AS type,
                    (SELECT value.string_value FROM UNNEST(event_params) WHERE key ='item_id') AS item_id,
                    CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key ='quantity') AS NUMERIC) AS quantity,
                    REPLACE((SELECT value.string_value FROM UNNEST(event_params) WHERE key ='source'), "}}","") AS source
                FROM `sipherg1production.analytics_387396350.events_{formatted_previous_date}` 
                WHERE event_name = 'inventory_balancing_update'
            )

            ,banner_key_raw AS
            (
                SELECT DISTINCT
                    event_timestamp,
                    user_id,
                    trace_id,
                    event_date,
                    SPLIT(source, ":")[OFFSET(1)] AS banner_type,
                    item_id AS key_type,
                    quantity AS key_quantity,
                FROM raw
                WHERE LOWER(item_id) LIKE '%key%' AND type = 'out'
            )

            ,item_from_banner_raw AS
            (
                SELECT DISTINCT
                    event_timestamp,
                    user_id,
                    trace_id,
                    event_date,
                    SPLIT(source, ":")[OFFSET(1)] AS banner_type,
                    REPLACE(item_id, ".", "_") AS item_id,
                    item_type,
                    quantity AS item_quantity,
                FROM raw
                WHERE LOWER(source) LIKE("%banner%") AND LOWER(item_id) NOT LIKE '%key%'
            )

            ,final AS
            (
                SELECT
                    item_from_banner_raw.event_timestamp AS event_timestamp,
                    user_id,
                    trace_id,
                    event_date,
                    key_quantity,
                    'client' AS data_source,
                    SUM(item_quantity) OVER (PARTITION BY user_id, trace_id) AS total_character_unlock,
                    CASE
                    WHEN item_id = 'NEKO_Normal' THEN 'NEKO_Uncommon'
                    WHEN item_id = 'INU_Normal' THEN 'INU_Uncommon'
                    WHEN item_id = 'BURU_Normal' THEN 'BURU_Uncommon'
                    WHEN item_id = 'NEKO_Cyborg' THEN 'NEKO_Rare'
                    WHEN item_id = 'INU_Elemental' THEN 'INU_Rare'
                    WHEN item_id = 'BURU_Cyborg' THEN 'BURU_Rare'
                    WHEN item_id = 'NEKO_Supernatural' THEN 'NEKO_Epic'
                    WHEN item_id = 'INU_Cyborg' THEN 'INU_Epic'
                    WHEN item_id = 'BURU_Elemental' THEN 'BURU_Epic'
                    WHEN item_id = 'NEKO_Elemental' THEN 'NEKO_Legendary'
                    WHEN item_id = 'INU_Cyborg' THEN 'INU_Legendary'
                    WHEN item_id = 'BURU_Supernatural' THEN 'BURU_Legendary'
                    END AS item_id,
                    item_quantity,
                FROM item_from_banner_raw
                LEFT JOIN banner_key_raw USING(user_id, trace_id, event_date)
                WHERE item_from_banner_raw.banner_type = 'Vessel_Banner' AND item_type = 'Character'
            )

            ,client AS 
            (
                SELECT 
                    event_timestamp,
                    user_id,
                    trace_id,
                    item_id,
                    item_quantity AS updated_balance,
                    data_source
                FROM 
                    final 
                WHERE 
                    total_character_unlock >= 2
                    AND item_id IS NOT NULL
                    AND TIMESTAMP_MICROS(event_timestamp) BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE) AND CURRENT_TIMESTAMP()
                ORDER BY user_id, trace_id
            )

            ,raw_2 AS
            (
                SELECT 
                    *
                FROM 
                    `sipher-data-platform.raw_game_meta.raw_inventory_balancing_update` 
                WHERE 
                    updated_balance_date = CURRENT_DATE()
                    AND TIMESTAMP_MICROS(CAST(updated_balance_timestamp AS INT64)*10 - 62135596800000000)  BETWEEN DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE) AND  CURRENT_TIMESTAMP()
            )

            ,key_balancing AS 
            (
                SELECT
                    * EXCEPT(updated_balance),
                    LAG(updated_balance) OVER (PARTITION BY user_id, instance_id ORDER BY updated_balance_timestamp) AS before_updated_balance,
                    updated_balance,
                    COALESCE(CAST(updated_balance AS INT64), 0) - COALESCE(CAST(LAG(updated_balance) OVER (PARTITION BY user_id, instance_id ORDER BY updated_balance_timestamp) AS INT64),0) AS in_balance,
                FROM 
                    raw_2
                WHERE LOWER(instance_id) LIKE '%key%'
            )

            ,character_cnt AS
            (
                SELECT
                    trace_id,
                    item_type,
                    SUM(CAST(updated_balance AS INT64)) AS updated_balance
                FROM raw_2
                WHERE UPPER(trace_id) IN
                (SELECT
                    DISTINCT UPPER(trace_id) AS trace_id
                FROM key_balancing
                WHERE in_balance < 0)
                AND LOWER(item_type) = 'character'
                GROUP BY 1,2
            )

            ,server AS 
            (
                SELECT
                    CAST(updated_balance_timestamp AS INT64 ) AS event_timestamp,
                    user_id,
                    trace_id,
                    item_sub_type AS item_id,
                    CAST(updated_balance AS INT64) AS updated_balance,
                    'server' AS data_source
                FROM 
                    raw_2
                WHERE UPPER(trace_id) IN (SELECT DISTINCT UPPER(trace_id) AS trace_id FROM key_balancing WHERE in_balance IN (-1, -10))
                AND UPPER(trace_id) IN (SELECT DISTINCT UPPER(trace_id) AS trace_id FROM character_cnt WHERE updated_balance >= 2)
                AND LOWER(item_type) = 'character'
                ORDER BY trace_id
            )

            ,joined AS 
            (
                SELECT  
                    a.*,
                    b.*
                FROM 
                    client a
                RIGHT JOIN
                    server b
                ON 
                    a.user_id = b.user_id
                    AND a.trace_id = b.trace_id
            )

            SELECT
                *
            FROM
            (
                SELECT DISTINCT * FROM client
                UNION ALL 
                SELECT DISTINCT * FROM server
            )
            ORDER BY 
                user_id, trace_id


    """    


    logging.info(f'QUERY: {query}')

    service_account_json_path = BaseHook.get_connection(gcp_conn_id).extra_dejson["key_path"]
    bq_client = bigquery.Client.from_service_account_json(service_account_json_path)
    
    try:
        query_job = bq_client.query(query, project=bigquery_project)
        df = query_job.to_dataframe()
        data = df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        data = []

    return data



@task(task_id='send_alerting')
def send_alerting(
    gcp_conn_id: str,
    bigquery_project: str,
    ds: str
):
 

    data = _get_data(ds=ds, gcp_conn_id=gcp_conn_id,bigquery_project=bigquery_project)
    logging.info(f'DATA: {data}')

    user_trace_counts = defaultdict(lambda: defaultdict(int))

    for entry in data:
        
        user_id = entry['user_id']
        trace_id = entry['trace_id']
        character_cnt = int(entry['updated_balance'])
        user_trace_counts[user_id][trace_id] += character_cnt

    for user_id, trace_counts in user_trace_counts.items():
        for trace_id, count in trace_counts.items():
            if count >= 3:
           
         
                characters = [f"{entry['item_id']}({int(float(entry['updated_balance']))})" for entry in data if entry['user_id'] == user_id and entry['trace_id'] == trace_id]
                data_sources = set(entry['data_source'] for entry in data if entry['user_id'] == user_id and entry['trace_id'] == trace_id)
                data_source_text = ', '.join(data_sources)
                message_text = f"`{user_id}` has just successfully opened *{count}* Vessels, includes: `{' - '.join(characters)}`"
                tx_id_message = f"tx_id: `{trace_id}`\n Data Source: `{data_source_text}`"

                blocks = [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": ":alert: Sipher Odyssey Alert :alert:"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Please Check*: {users}",
                        }
                    },
                    {"type": "divider"},
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message_text
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f'{tx_id_message}'
                        }
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "View Looker Dashboard"},
                                "style": "primary",
                                "url": dashboard_url,
                            }
                        ],
                    },
                    {"type": "divider"},
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": "*Data provided by Bot*"
                            },
                            {
                                "type": "image",
                                "image_url": "https://cdn-icons-png.flaticon.com/512/9299/9299874.png",
                                "alt_text": "icon"
                            },
                            {
                                "type": "plain_text",
                                "text": f"{ds}",
                                "emoji": True
                            }
                        ]
                    },
                    {
                        "type": "divider"
                    }
                ]

                client.chat_postMessage(
                    channel=channel, 
                    text=message_text, 
                    blocks=blocks
                )
