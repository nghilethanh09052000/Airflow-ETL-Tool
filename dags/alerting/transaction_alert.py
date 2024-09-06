from datetime import datetime

from airflow import DAG
from airflow import settings
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from alerting.scripts.check_new_transaction import \
    check_transaction_alert
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value


BIGQUERY_PROJECT = Variable.get("bigquery_project")

start_date = set_env_value(production=datetime(2022, 11, 18), dev=datetime(2022, 11, 16))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="0,20,40 * * * *", dev="@once")

default_args = {
    "owner": "nghi.le",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}


with DAG(
    dag_id="transaction_alert",
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    template_searchpath=[settings.DAGS_FOLDER + '/alerting/query'],
) as dag:

    @task(templates_exts=[".sql"])
    def check_transaction_alert_task(query):
        check_transaction_alert(query=query)

    add_new_transaction = BigQueryInsertJobOperator(
        task_id="add_new_transaction",
        gcp_conn_id="sipher_gcp",
        configuration={
            "query": {
                "query": "{% include 'query/add_new_transaction.sql' %}",  
                "useLegacySql": False
            }
        },
        params={"bq_project": BIGQUERY_PROJECT},
        dag=dag,
    )

    add_new_transaction >> check_transaction_alert_task(query='check_new_transaction.sql')
