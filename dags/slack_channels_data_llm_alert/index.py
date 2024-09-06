from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from datetime import datetime, timedelta
from google.cloud import bigquery
from anthropic import AnthropicBedrock
from slack_sdk import WebClient
from typing import Dict, List
import logging
import json
import httpx


DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 28),
    dev=datetime(2024, 5, 28)
)

DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 1 * * *", dev="0 1 * * *")

GCP_CONN_ID = "sipher_gcp"
GCP_PREFIX = 'slack_data'
BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
BIGQUERY_PROJECT = Variable.get("bigquery_project")
USER = ', '.join([f"@{name}" for name in Variable.get('slack_channels_data_llm_alert_user_abnormal').split(', ')])

BQ_DATASET = 'slack_data'
SLACK_ASSISTANT_CHANNEL_ID = 'C0750L20WNL'
CHANNEL_IDS = [
    {
        'channel_id': 'C0289Q92DRV',
        'channel_title': 'Game Learning Resource'
    }
]

def get_data_from_bg(channel_id):
    service_account_json_path = BaseHook.get_connection(
        GCP_CONN_ID).extra_dejson["key_path"]
    bq_client = bigquery.Client.from_service_account_json(
        service_account_json_path)
    query = f""" 
        WITH
            ranked_messages AS (
                SELECT
                    client_msg_id,
                    author,
                    message,
                    urls,
                    tags,
                    DATE(TIMESTAMP_SECONDS(CAST(SPLIT(created, '.')[OFFSET(0)] AS INT64))) AS created,
                    ROW_NUMBER() OVER (PARTITION BY client_msg_id, channel_id, author, message ORDER BY __collected_ts DESC) AS row_num
                FROM
                    `sipher-data-platform.slack_data.slack_messages`
                WHERE
                    DATE(TIMESTAMP_SECONDS(CAST(SPLIT(created, '.')[OFFSET(0)] AS INT64))) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    AND channel_id =  '{channel_id}'
                    
            )

            ,permalink_data AS (
                SELECT
                    client_msg_id,
                    permalink
                FROM
                    `sipher-data-platform.slack_data.slack_permanent_links` 
                )

            ,user AS (
                SELECT
                    id,
                    name
                FROM
                    `sipher-data-platform.slack_data.slack_users`
            )

            ,final AS (
                SELECT
                    ranked_messages.message,
                    ranked_messages.urls AS url,
                    permalink_data.permalink AS post_link,
                    user.name as author,
                    CAST(created AS STRING) AS created
                FROM
                    ranked_messages
                JOIN permalink_data
                ON ranked_messages.client_msg_id = permalink_data.client_msg_id
                JOIN user
                ON ranked_messages.author = user.id
                WHERE
                    ranked_messages.row_num = 1
                
            )

            SELECT DISTINCT * FROM final
    """
    logging.info(f"Query Processing {query}")

    query_job = bq_client.query(query, project=BIGQUERY_PROJECT)
    df = query_job.to_dataframe()
    data = df.to_dict(orient='records')
    msg = json.dumps(data)

    return msg


def get_llm_results(msg, channel_title):
    client = AnthropicBedrock(
        aws_access_key=Variable.get(
            'slack_channels_data_llm_alert_aws_access_token'),
        aws_secret_key=Variable.get(
            'slack_channels_data_llm_alert_aws_secret_token'),
        aws_region="us-east-1"
    )

    prompt = f"You are a summary expert of Game Learning Resource News. Please summary all News from Slack Channel Game-Learning-Resource bewlo: {msg}. Please fill News from 1 to N and short summary, and you should be attacked each News with url, and include who is mentioned in this News. \
        Output Format: \
        1. Title \
        - Summary:  \
        - Url: \
        - Post Link: \
        - Author: \
        - Created at:"
    message = client.messages.create(
        model="anthropic.claude-3-sonnet-20240229-v1:0",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )

    results = message.content[0].text
    return results

@task(task_id='alerting_service')
def alerting_service(
    channel:  Dict[str, str],
    **kwargs
):

    msg = get_data_from_bg(channel.get('channel_id'))
    results = get_llm_results(msg, channel.get('channel_title'))
    slack_token = Variable.get('slack_sipher_odyssey_alerting_token')
    client = WebClient(token=slack_token)

    blocks = [
        {
            "type": "header",
            "text": {
                    "type": "plain_text",
                    "text": ":slot_machine: Game Learning Resource Summary :slot_machine:"
            }
        },
        {
            "type": "section",
            "text": {
                    "type": "mrkdwn",
                    "text": f"*Please Check*: {USER}",
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": results
            }
        },
        {"type": "divider"},
    ]

    client.chat_postMessage(
        channel=SLACK_ASSISTANT_CHANNEL_ID,
        text=results,
        blocks=blocks
    )

   
def get_top_hackernews_stories(num_stories: int = 20) -> str:
    """Use this function to get top stories from Hacker News.

    Args:
        num_stories (int): Number of stories to return. Defaults to 10.

    Returns:
        str: JSON string of top stories.
    """

    # Fetch top story IDs
    response = httpx.get("https://hacker-news.firebaseio.com/v0/topstories.json")
    story_ids = response.json()

    # Fetch story details
    stories = []
    for story_id in story_ids[:num_stories]:
        story_response = httpx.get(f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json")
        story = story_response.json()
        if "text" in story:
            story.pop("text", None)
        stories.append(story)
    return json.dumps(stories)


def get_hack_news_results(news_cnt):
    client = AnthropicBedrock(
        aws_access_key=Variable.get(
            'slack_channels_data_llm_alert_aws_access_token'),
        aws_secret_key=Variable.get(
            'slack_channels_data_llm_alert_aws_secret_token'),
        aws_region="us-east-1"
    )

    result = get_top_hackernews_stories(news_cnt)

    prompt = f"You are expert in Summary News. Please summary {news_cnt} News from Hackernews, please struct item from 1 to {news_cnt}, each item attached url from each item: {result}. \
        Format output: \
        1. title \
        Summary: \
        - Url: "
        
    message = client.messages.create(
        model="anthropic.claude-3-sonnet-20240229-v1:0",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )

    results = message.content[0].text
    return results
   

@task(task_id='alerting_hackernews')
def alerting_hackernews():
    
    results = get_hack_news_results(10)
    logging.info(f"LLM RESULTS--------------------------- {results}")

    slack_token = Variable.get('slack_sipher_odyssey_alerting_token')
    client = WebClient(token=slack_token)
    items = results.split('\n\n')  
    structured_items = []

    for item in items:
        lines = item.split('\n')
        if len(lines) > 1:
            title = lines[0].strip()
            summary = lines[2].strip() if len(lines) > 2 else ""
            url = lines[-1].strip().split(": ")[-1] if "Url: " in lines[-1] else ""
            structured_items.append({
                "title": title,
                "summary": summary,
                "url": url
            })

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":rolled_up_newspaper: HackerNews Summary :rolled_up_newspaper:"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Please Check*: {USER}",
            }
        },
        {"type": "divider"},
    ]

    for item in structured_items:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{item['title']}*\n>{item['summary']}\n- Url: {item['url']}"
            }
        })
        blocks.append({"type": "divider"})

    client.chat_postMessage(
        channel=SLACK_ASSISTANT_CHANNEL_ID,
        blocks=blocks
    )



default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 8,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_callback,
}


with DAG(
    dag_id="slack_channels_data_llm_alert",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["Slack", "Channel", "LLM", "Alert"],
    catchup=False
) as dag:

    alerting_service \
        .partial() \
        .expand(channel=CHANNEL_IDS)
    
    alerting_hackernews()