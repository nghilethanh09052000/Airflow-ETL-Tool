import logging
from airflow import settings
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from utils.alerting.slack import SlackApp
from utils.common import set_env_value
from utils.constants import SipherSlackChannelID
from google.cloud import bigquery

CHANNEL_ID = set_env_value(
    production=SipherSlackChannelID.TOP_GAME, dev=SipherSlackChannelID.TEST
)
bigquery_billing_project = Variable.get('bigquery_billing_project')


def top_apps_alert(query, **kwargs):

    client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection("sipher_gcp").extra_dejson[
            "key_path"
        ]
    )

    logging.info("Run query check...")
    print(query)

    res = client.query(query, project=bigquery_billing_project).result()

    if res.total_rows > 0:
        logging.info("Post message to Slack")
        slack = SlackApp(channel_id=CHANNEL_ID)

        block = [{'type': 'header', 'text': {'type': 'plain_text', 'text': 'TOP APP TODAY'}},
                {"type": "section", 
                  "fields": [{"type": "mrkdwn", "text": "*STATUS:*"},
                             {"type": "mrkdwn", "text": "*APP ID*"}]
                             }]
        for row in res:    
            msg_dict = {
                        "type": "section",
                        "fields": 
                            [
                                {"type": "mrkdwn",
                                "text": f"`{row.today_rank} | {row.rank_status}`"}
                            ],
                        "accessory": 
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": f"{row.app_name}"
                                },
                                "url": f"https://app.sensortower.com/overview/{row.app_id}?metric=units&tab=about&utab=summary",
                            }
                        }   
            
            block.append(msg_dict)

        attachment = [
            {
                "fallback": "Top apps",
                "color": "#1CF847",
                "blocks": block
            }
        ]

        slack.post_message(attachments=attachment)