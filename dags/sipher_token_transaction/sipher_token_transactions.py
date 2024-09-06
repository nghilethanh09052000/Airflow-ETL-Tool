from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from utils.dbt import default_args_for_dbt_operators

from utils.alerting.airflow import airflow_callback

EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

DEFAULT_ARGS = {
    "owner": "dinh.nguyen",
    "start_date": datetime(2021, 9, 7),
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)
# schedule_interval = '0 */3 * * *'
schedule_interval = "@daily"

dag = DAG(
    dag_id="sipher_token_transactions",
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    max_active_runs=1,
    tags=["ethereum", "presentation"],
    catchup=False,
)

sipher_token_transfers = BigQueryInsertJobOperator(
    task_id="sipher_token_transfers",
    gcp_conn_id="sipher_gcp",
    configuration={
        "query": {
            "query": "{% include 'query/sipher_token_transfers.sql' %}",  
            "useLegacySql": False
        }
    },
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

token_transaction_sipherians = BigQueryInsertJobOperator(
    task_id="token_transaction_sipherians",
    gcp_conn_id="sipher_gcp",
    configuration={
        "query": {
            "query": "{% include 'query/token_transaction_sipherians.sql' %}",  
            "useLegacySql": False
        }
    },
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

transactions_sipherians = BigQueryInsertJobOperator(
    task_id="transactions_sipherians",
    gcp_conn_id="sipher_gcp",
    configuration={
        "query": {
            "query": "{% include 'query/sipherians.sql' %}",  
            "useLegacySql": False
        }
    },
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

sipher_token_onwer_by_time = BigQueryInsertJobOperator(
    task_id="sipher_token_onwer_by_time",
    gcp_conn_id="sipher_gcp",
    configuration={
        "query": {
            "query": "{% include 'query/sipher_token_owner_by_time.sql' %}",  
            "useLegacySql": False
        }
    },
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

sipher_token_onwer_by_time_all = DbtRunOperator(
    task_id="sipher_token_onwer_by_time_all",
    models="sipher_token_onwer_by_time_all",
    vars={"ds": "{{ ds }}"},
    dag=dag,
)

finance_wallet_transaction = BigQueryInsertJobOperator(
    task_id="finance_wallet_transaction",
    gcp_conn_id="sipher_gcp",
    configuration={
        "query": {
            "query": "{% include 'query/finance_wallet_transaction.sql' %}",  
            "useLegacySql": False
        }
    },
    dag=dag,
)

atherlabs_dashboard_DAU = DbtRunOperator(
    task_id="atherlabs_dashboard_DAU",
    models="atherlabs_dashboard_DAU",
    dag=dag,
)

token_transaction_sipherians >> transactions_sipherians
sipher_token_transfers >> sipher_token_onwer_by_time >> sipher_token_onwer_by_time_all
finance_wallet_transaction
atherlabs_dashboard_DAU
