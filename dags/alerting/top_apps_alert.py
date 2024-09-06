from datetime import datetime
from airflow import DAG
from airflow import settings
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from alerting.scripts.check_top_apps import top_apps_alert
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from sensor_tower.scripts.apps_metadata import AppsMetadata


start_date = set_env_value(production=datetime(2023, 5, 26), dev=datetime(2024, 1, 3))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="0 3 * * *", dev="@once")

BIGQUERY_PROJECT = Variable.get("bigquery_project")
APP_METADATA = AppsMetadata()

default_args = {
    "owner": "tri.nguyen",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}


with DAG(
    dag_id="top_apps_alert",
    default_args=default_args,
    template_searchpath=[settings.DAGS_FOLDER + '/alerting/query'],
    catchup=False,
    schedule_interval=schedule_interval,
) as dag:
    
    run_app_metadata = PythonOperator(
        task_id="run_app_metadata",
        dag=dag,
        provide_context=True,
        python_callable=APP_METADATA.run
    )

    @task(templates_exts=[".sql"])
    def top_apps_alert_task(query):
        top_apps_alert(query)

    update_daily_top_apps = BigQueryInsertJobOperator(
        task_id="update_daily_top_apps",
        gcp_conn_id="sipher_gcp",
        configuration={
            "query": {
                "query": "{% include 'query/update_daily_top_apps.sql' %}",  
                "useLegacySql": False
            }
        },
        params={"bq_project": BIGQUERY_PROJECT},
        dag=dag,
    )

    run_app_metadata >> update_daily_top_apps >> top_apps_alert_task('check_top_apps_new_ranking.sql')