import logging
import json
import requests
from google.cloud import bigquery
from airflow.models import Variable
from airflow.hooks.base import BaseHook

bigquery_billing_project = Variable.get('bigquery_billing_project')
query = '''
    SELECT * FROM `sipher-data-platform.reporting_social.adhoc_social_followers`
'''

def get_followers_data_from_bigquery():
    client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection("sipher_gcp").extra_dejson[
            "key_path"
        ]
    )

    logging.info("Run query check...")
    print(query)

    res = client.query(query, project=bigquery_billing_project).result()
    return next(res).total_followers

def post_followers_data():

    url = 'https://be.playsipher.com/api/socials/followers'
    total_followers = get_followers_data_from_bigquery()

    headers = {
        'Authorization': 'internal',
        'Content-Type': 'application/json' 
    }

    body = {
        'count': total_followers
    }

    response = requests.post(url, headers=headers, data=json.dumps(body))

    print("Response:", response.json()) 