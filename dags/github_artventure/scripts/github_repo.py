import json
import logging
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import bigquery
from google.oauth2 import service_account


__all__ = ["create_github_repo_table"]


BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")
EXECUTION_ENVIRONMENT = Variable.get("execution_environment", default_var="dev")


PROJECT_ID = "data-platform-387707"
# PROJECT_ID = "sipher-data-testing"
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

token = Variable.get("github_artventure_token")
headers = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28",
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


def create_github_repo_table():
    response = requests.get(
        "https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler",
        headers=headers,
    )
    data = response.json()

    selected_cols = [
        "id",
        "name",
        "full_name",
        "size",
        "stargazers_count",
        "watchers_count",
        "forks_count",
        "open_issues_count",
        "forks",
        "open_issues",
        "watchers",
        "network_count",
        "subscribers_count",
    ]
    selected_data = {col: data[col] for col in selected_cols}
    df = pd.DataFrame([selected_data])

    df.reset_index(drop=True, inplace=True)
    df["id"] = df["id"].astype(str)
    schema_config = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("size", "INTEGER"),
        bigquery.SchemaField("stargazers_count", "INTEGER"),
        bigquery.SchemaField("watchers_count", "INTEGER"),
        bigquery.SchemaField("forks_count", "INTEGER"),
        bigquery.SchemaField("open_issues_count", "INTEGER"),
        bigquery.SchemaField("forks", "INTEGER"),
        bigquery.SchemaField("open_issues", "INTEGER"),
        bigquery.SchemaField("watchers", "INTEGER"),
        bigquery.SchemaField("network_count", "INTEGER"),
        bigquery.SchemaField("subscribers_count", "INTEGER"),
    ]

    logging.info("Done creating github_repo_table")
    upload_data_to_bq(
        df,
        table_name=f"github_repo_stats_{formatted_date}",
        insert_type="replace",
        schema=schema_config,
    )
