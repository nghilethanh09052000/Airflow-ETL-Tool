import logging

from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import bigquery
from utils.alerting.slack import SlackApp
from utils.common import set_env_value
from utils.constants import SipherSlackChannelID

CHANNEL_ID = set_env_value(
    production=SipherSlackChannelID.TRANSACTION, dev=SipherSlackChannelID.TEST
)
BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")

def _check_name(name):
    return f"({name})" if name else ""

def check_transaction_alert(query, **kwargs):
    bigquery.Client.SCOPE += ("https://www.googleapis.com/auth/drive",)
    client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection("sipher_gcp").extra_dejson[
            "key_path"
        ]
    )

    logging.info("Run query check...")
    print(query)

    res = client.query(query, project=BIGQUERY_BILLING_PROJECT).result()

    if res.total_rows > 0:
        logging.info("Post warning message to Slack")
        slack = SlackApp(channel_id=CHANNEL_ID)
        recipients = Variable.get("sipher_wallet_transaction_recipients")
        pic = " ".join(f"<@{i}>" for i in recipients.replace(" ", "").split(","))
        slack.post_message(text=f"*New transaction* {pic}")

        for _, row in enumerate(res):
            attachment = [
                {
                    "fallback": "New transaction",
                    "color": "#FFA500",
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*tx_hash*: `{row.transaction_hash}` \n"
                                + f"*from*: `{row.from_address}` {_check_name(row.from_address_name)} \n"
                                + f"*to*: `{row.to_address}` {_check_name(row.to_address_name)} \n"
                                + f"*block time*: {row.block_timestamp} \n"
                                + f"*value*: {row.converted_value} {row.symbol} "
                            },
                        },
                        {
                            "type": "actions",
                            "elements": [
                                {
                                    "type": "button",
                                    "text": {
                                        "type": "plain_text",
                                        "text": "View on Etherscan",
                                    },
                                    "style": "primary",
                                    "url": f"https://etherscan.io/tx/{row.transaction_hash}",
                                }
                            ],
                        },
                    ],
                }
            ]
            slack.post_message(attachments=attachment)
