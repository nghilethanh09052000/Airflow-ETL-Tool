import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas import json_normalize

__all__ = ["create_repo_stars_history_table"]


BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")
EXECUTION_ENVIRONMENT = Variable.get("execution_environment", default_var="dev")


PROJECT_ID = "data-platform-387707"
DATASET_ID = "github_artventure"

SERVICE_ACCOUNT_JSON_PATH = BaseHook.get_connection("sipher_gcp").extra_dejson[
    "key_path"
]
CREDENTIALS = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_JSON_PATH
)
CLIENT = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)
logging.info("Done BQ config")

now = datetime.now()
formatted_date = now.strftime("%Y%m%d")

token = Variable.get("github_artventure_token", default_var="dev")
headers = {
    "Accept": "application/vnd.github.v3.star+json",
    "Authorization": f"token {token}",
}


def upload_data_to_bq(
    df,
    table_name="repo_clones",
    bq_dataset=DATASET_ID,
    insert_type="append",
    schema=None,
):
    dataset_id = bq_dataset
    table_id = table_name
    table_ref = CLIENT.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    if insert_type == "append":
        job_config.write_disposition = (
            bigquery.WriteDisposition.WRITE_APPEND
        )  # Choose the append disposition as per your needs
    else:
        job_config.write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
        )  # Choose the write disposition as per your needs
    job_config.schema = schema

    job = CLIENT.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    logging.info("Done uploading to BQ")


def get_repo_stargazers(repo, token, page=1):
    url = f"https://api.github.com/repos/{repo}/stargazers?page={page}"
    headers = {
        "Accept": "application/vnd.github.v3.star+json",
        "Authorization": f"token {token}",
    }
    response = requests.get(url, headers=headers)
    return response.json()


def get_daily_unique_stars(repo, token):
    page = 1
    daily_unique_stars = defaultdict(int)

    while True:
        stargazers = get_repo_stargazers(repo, token, page)
        if not stargazers:
            break

        for stargazer in stargazers:
            starred_at = stargazer["starred_at"]
            date = starred_at[:10]  # Extract the date from the timestamp
            daily_unique_stars[date] += 1

        page += 1

    df = pd.DataFrame(list(daily_unique_stars.items()), columns=["date", "count"])
    return df


def create_repo_stars_history_table():
    df = get_daily_unique_stars("ArtVentureX/sd-webui-agent-scheduler", token)
    df["date"] = df["date"].astype(str)
    df["count"] = pd.to_numeric(df["count"])
    schema_config = [
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("count", "INTEGER"),
    ]
    upload_data_to_bq(
        df,
        table_name=f"github_stars_history",
        insert_type="replace",
        schema=schema_config,
    )
