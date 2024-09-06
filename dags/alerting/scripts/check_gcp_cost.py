import logging

from google.cloud import bigquery
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from utils.alerting.slack import SlackApp
from utils.common import set_env_value
from utils.constants import SipherSlackChannelID

CHANNEL_ID = set_env_value(
    production=SipherSlackChannelID.INFRA_COST, dev=SipherSlackChannelID.TEST
)

BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")


def check_daily_cost_is_not_larger_than_double_avg_7d(query, **kwargs):

    client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection("sipher_gcp").extra_dejson["key_path"]
    )

    logging.info("Run query check...")
    print(query)

    res = client.query(query, project=BIGQUERY_BILLING_PROJECT).result()

    if res.total_rows > 0:
        slack = SlackApp(channel_id=CHANNEL_ID)
        logging.info("Post warning message to Slack")
        slack.post_message("GCP cost is double than average 7 days. Please check!!!!")
    else:
        logging.info("Nothing unusual")
