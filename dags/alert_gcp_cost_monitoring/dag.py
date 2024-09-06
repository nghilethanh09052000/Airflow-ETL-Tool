from airflow import DAG
from airflow.models import Variable
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from datetime import datetime, timedelta
from alert_gcp_cost_monitoring._index import BigQueryUsageAlerting

DAG_START_DATE = set_env_value(
    production=datetime(2024, 5, 22), 
    dev=datetime(2024, 5, 7)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_project")



default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_callback,
    'provide_context': True
}


with DAG(
    dag_id="alert_gcp_cost_monitoring",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["Alerting", "BigQuery", "Monitoring"],
    catchup=False
) as dag:


    check_total_today_cost = BigQueryUsageAlerting(
        task_id = 'check_total_today_cost' , 
        gcp_conn_id=GCP_CONN_ID, 
        project_id=BIGQUERY_PROJECT, 
        ds = '{{ds}}'
    )

    check_total_today_cost_compare_one_week = BigQueryUsageAlerting(
        task_id = 'check_total_today_cost_compare_one_week' , 
        gcp_conn_id=GCP_CONN_ID, 
        project_id=BIGQUERY_PROJECT, 
        ds = '{{ds}}'
    )

    check_total_today_cost_per_project_compare_one_week = BigQueryUsageAlerting(
        task_id = 'check_total_today_cost_per_project_compare_one_week' , 
        gcp_conn_id=GCP_CONN_ID, 
        project_id=BIGQUERY_PROJECT, 
        ds = '{{ds}}'
    )

    check_query_cost_compute_greater_than_five = BigQueryUsageAlerting(
        task_id = 'check_query_cost_compute_greater_than_five' , 
        gcp_conn_id=GCP_CONN_ID, 
        project_id=BIGQUERY_PROJECT, 
        ds = '{{ds}}'
    )

    check_top_five_user_most_compute_query = BigQueryUsageAlerting(
        task_id = 'check_top_five_user_most_compute_query' , 
        gcp_conn_id=GCP_CONN_ID, 
        project_id=BIGQUERY_PROJECT, 
        ds = '{{ds}}'
    )

 
       