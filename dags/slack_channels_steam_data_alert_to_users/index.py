from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from datetime import datetime, timedelta
from google.cloud import bigquery
from slack_sdk import WebClient
from typing import List, Dict
import yaml
import logging
import pandas as pd
import os

DAG_START_DATE = set_env_value(
    production=datetime(2024, 7, 30),
    dev=datetime(2024, 7, 30)
)

DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")

GCP_CONN_ID = "sipher_gcp"
CHANNEL_ID = 'C07EEPE1NAJ'
USER = ', '.join(
    [f"@{name}" for name in Variable.get('slack_channels_steam_data_alert_to_users').split(', ')])


default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_callback,
}


def load_filters_from_yaml() -> List[Dict[str, str]]:
    dag_folder = os.path.dirname(__file__)
    yaml_path = os.path.join(dag_folder, '_rules.yaml')
    with open(yaml_path, 'r') as file:
        data = yaml.safe_load(file)
    return data['rules']


def read_sql_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        query = file.read()
    return query


   
def send_slack_alert(title: str, message: str, steam_id: str, steam_name: str):
    dashboard_url = f"https://steamdb.info/app/{steam_id}/charts/"
    client = WebClient(token=Variable.get(
        'slack_sipher_odyssey_alerting_token'))
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": title
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Please Check*: {USER}",
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{steam_name}* - https://shared.cloudflare.steamstatic.com/store_item_assets/steam/apps/{steam_id}/header.jpg",
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Steam App"},
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
                }
            ]
        },
        {
            "type": "divider"
        }
    ]
    response = client.chat_postMessage(
        channel=CHANNEL_ID,
        blocks=blocks
    )
    logging.info(
        f"Message sent to Slack channel {CHANNEL_ID}: {response['ts']}")


""" RUNNING TASKS """

def fetch_data_from_bigquery(**kwargs):
    client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection(GCP_CONN_ID).extra_dejson["key_path"])
    dag_folder = os.path.dirname(__file__)
    sql_path = os.path.join(dag_folder, 'query', 'index.sql')
    query = read_sql_file(sql_path)
    query_job = client.query(query)
    results = query_job.result()

    data = []
    for row in results:
        row_dict = dict(row)
        row_dict = {key: value.strftime('%Y-%m-%d') if isinstance(value, datetime) else value for key, value in row_dict.items()}
        data.append(row_dict)

    df = pd.DataFrame(data)
    json_data = df.to_json(orient='records')
    kwargs['ti'].xcom_push(key='bq_data', value=json_data)
    return 'Data fetched and pushed to XCom'

def apply_filters_and_send_alerts(
    filter_name: str,
    condition: str,
    slack_info: Dict[str, str],
    **kwargs
):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_data_from_bigquery', key='bq_data')
    df = pd.read_json(json_data)
    filtered_df = df.query(condition)
    if not filtered_df.empty:
        title = slack_info.get('title', 'Alert')
        message_format = slack_info.get('message', 'No message format provided')
        for _, row in filtered_df.iterrows():
            message = message_format.format(**row)
            steam_id = row.get('steamId', '')
            steam_name = row.get('name', '')
            send_slack_alert(title, message, steam_id, steam_name)
    else:
        logging.info('No filter matched.')


with DAG(
    dag_id="slack_channels_steam_data_alert_to_users",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["Slack", "Channel", "Steam", "Data"],
    catchup=False
) as dag:
    
    get_steam_data = BashOperator(
        task_id=f"get_steam_data",
        bash_command=f"""
            cd /home/airflow/dags/slack_channels_steam_data_alert_to_users && \
            scrapy crawl steam  
        """
    )
    
    fetch_data = PythonOperator(
        task_id='fetch_data_from_bigquery',
        python_callable=fetch_data_from_bigquery,
        provide_context=True
    )

    filter_tasks = [
        PythonOperator(
            task_id=f"filter_and_alert_{rule['name']}",
            python_callable=apply_filters_and_send_alerts,
            op_args=[
                rule["name"],
                rule["condition"],
                rule.get("slack", [{}])[0]
            ],
            provide_context=True,
            dag=dag
        )
        for rule in load_filters_from_yaml()
    ]

    get_steam_data >> fetch_data >> filter_tasks
