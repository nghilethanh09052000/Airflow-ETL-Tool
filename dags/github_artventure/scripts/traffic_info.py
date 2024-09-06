import logging
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import bigquery
from google.oauth2 import service_account


__all__ = [
    "create_repo_clones_table",
    "create_traffic_views_table",
    "create_traffic_popular_paths_table",
    "create_traffic_popular_referrers_table",
]

token = Variable.get("github_artventure_token")
headers = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28",
}
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


def create_repo_clones_table():
    response = requests.get(
        "https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler/traffic/clones",
        headers=headers,
    )

    data = response.json()
    df = pd.DataFrame(data["clones"])
    schema_config = [
        bigquery.SchemaField("timestamp", "STRING"),
        bigquery.SchemaField("count", "INTEGER"),
        bigquery.SchemaField("uniques", "INTEGER"),
    ]
    logging.info("Done creating repo_clones_table")
    upload_data_to_bq(
        df,
        table_name=f"repo_clones_{formatted_date}",
        insert_type="replace",
        schema=schema_config,
    )


def create_traffic_views_table():
    # response2 = requests.get('https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler/traffic/popular/paths', headers=headers)
    # PATH = 'traffic/popular/referrers'
    resp = requests.get(
        "https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler/traffic/views",
        headers=headers,
    )
    data = resp.json()
    df = pd.DataFrame(data["views"])
    schema_config = [
        bigquery.SchemaField("timestamp", "STRING"),
        bigquery.SchemaField("count", "INTEGER"),
        bigquery.SchemaField("uniques", "INTEGER"),
    ]
    logging.info("Done creating traffic_views_table")
    upload_data_to_bq(
        df,
        table_name=f"traffic_views_{formatted_date}",
        insert_type="replace",
        schema=schema_config,
    )


def create_traffic_popular_paths_table():
    # response2 = requests.get('https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler/traffic/popular/paths', headers=headers)
    # PATH = 'traffic/popular/referrers'
    resp = requests.get(
        "https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler/traffic/popular/paths",
        headers=headers,
    )

    data = resp.json()
    df = pd.DataFrame(data)
    schema_config = [
        bigquery.SchemaField("path", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("count", "INTEGER"),
        bigquery.SchemaField("uniques", "INTEGER"),
    ]
    logging.info("Done creating traffic_popular_paths_table")
    upload_data_to_bq(
        df,
        table_name=f"traffic_popular_paths_{formatted_date}",
        insert_type="replace",
        schema=schema_config,
    )


def create_traffic_popular_referrers_table():
    # response2 = requests.get('https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler/traffic/popular/paths', headers=headers)
    # PATH = 'traffic/popular/referrers'
    resp = requests.get(
        "https://api.github.com/repos/ArtVentureX/sd-webui-agent-scheduler/traffic/popular/referrers",
        headers=headers,
    )

    data = resp.json()
    df = pd.DataFrame(data)
    schema_config = [
        bigquery.SchemaField("referrer", "STRING"),
        bigquery.SchemaField("count", "INTEGER"),
        bigquery.SchemaField("uniques", "INTEGER"),
    ]
    logging.info("Done creating traffic_popular_referrers_table")
    upload_data_to_bq(
        df,
        table_name=f"traffic_popular_referrers_{formatted_date}",
        insert_type="replace",
        schema=schema_config,
    )
