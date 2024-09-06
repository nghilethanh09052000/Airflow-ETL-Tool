from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback


from sensor_tower.tasks.apps.task import (
    task_get_us_apps_data_with_app_ids_bq
)

from sensor_tower.tasks.creative_tops.task import (
    group_tasks_get_creative_tops_on_specific_os_countries_and_categories
)

from sensor_tower.tasks.sales_report_estimates_comparision_atrributes.task import (
    task_get_sales_report_estimates_comparision_attributes_on_specific_os_and_categories,
    task_get_sales_report_estimates_comparision_attributes_on_unified_os_and_categories
)

from sensor_tower.tasks.get_reviews.task import (
    task_get_get_review_us_country_on_specific_app_ids
)

from sensor_tower.tasks.demographics.task import (
    group_tasks_get_quaterly_demographic_data_in_app_ids
)
from sensor_tower.tasks.retention.task import (
    group_tasks_get_quaterly_retention_data_in_specific_app_ids
)
from sensor_tower.tasks.app_overlap.task import (
    task_get_worldwide_sevenday_app_overlap_data_on_specific_app_ids
)
from sensor_tower.tasks.active_users.task import (
    group_tasks_get_daily_and_monthly_active_users_data_on_specific_app_ids
)
from sensor_tower.tasks.time_spent.task import (
    task_get_monthly_time_spent_data_on_specific_app_id
)
from sensor_tower.tasks.sales_report_estimates.task import (
    task_get_daily_sales_report_estimates_data_on_specific_app_ids
)
from sensor_tower.tasks.download_by_sources.task import (
    task_get_download_by_source_all_countries_data_on_specific_app_ids
)



DAG_START_DATE = set_env_value(production=datetime(
    2024, 5, 22), dev=datetime(2023, 1, 1))
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@daily")


BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_sensortower"

BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
GCS_PREFIX = "sensortower"
GCP_CONN_ID = "sipher_gcp"


HTTP_CONN_ID = "sensortower"

default_args = {
    "owner": "nghi.le",
    "start_date": DAG_START_DATE,
    "trigger_rule": "all_done",
    "retries": 3,
    "retry_delay":  timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
    "task_concurrency": 3,
    "concurrency": 3,
    "max_active_tasks": 3
}



@task(task_id="create_big_lake_table")
def create_big_lake_table_task(bq_table_name, gcs_prefix, gcs_partition_expr):
    return create_external_bq_table_to_gcs(
        gcp_conn_id=GCP_CONN_ID,
        bq_project=BIGQUERY_PROJECT,
        bq_dataset=BQ_DATASET,
        bq_table=bq_table_name,
        gcs_bucket=BUCKET,
        gcs_object_prefix=gcs_prefix,
        gcs_partition_expr=gcs_partition_expr,
    )


with DAG(
    dag_id="sensortower",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["sensortower", "raw"],
    catchup=False
) as dag:

    partition_expr = "{snapshot_date:DATE}"

    """Refactor Based On Max Mediation"""

    create_big_lake_table_usage_intelligence_demographics_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_usage_intelligence_demographics_data"
    )(
        bq_table_name=f"raw_usage_intelligence_demographics",
        gcs_prefix=f"{GCS_PREFIX}/usage_intelligence/user_insights/demographics",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_usage_intelligence_retention_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_usage_intelligence_retention_data"
    )(
        bq_table_name=f"raw_usage_intelligence_retention",
        gcs_prefix=f"{GCS_PREFIX}/usage_intelligence/user_insights/retention",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_usage_intelligence_app_overlap_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_usage_intelligence_app_overlap_data"
    )(
        bq_table_name=f"raw_usage_intelligence_app_overlap",
        gcs_prefix=f"{GCS_PREFIX}/usage_intelligence/user_insights/app_overlap",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_usage_intelligence_active_users_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_usage_intelligence_active_users_data"
    )(
        bq_table_name=f"raw_usage_intelligence_active_users",
        gcs_prefix=f"{GCS_PREFIX}/usage_intelligence/usage/active_users",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_usage_intelligence_time_spent_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_usage_intelligence_time_spent_data"
    )(
        bq_table_name=f"raw_usage_intelligence_time_spent",
        gcs_prefix=f"{GCS_PREFIX}/usage_intelligence/session_analysis/time_spent",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_store_intelligence_sales_report_estimates_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_store_intelligence_sales_report_estimates_data"
    )(
        bq_table_name=f"raw_store_intelligence_sales_report_estimates",
        gcs_prefix=f"{GCS_PREFIX}/store_intelligence/download_and_revenues_estimates/sales_report_estimates",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_store_intelligence_downloads_by_sources_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_store_intelligence_downloads_by_sources_data"
    )(
        bq_table_name=f"raw_store_intelligence_downloads_by_sources",
        gcs_prefix=f"{GCS_PREFIX}/store_intelligence/downloads_by_source/downloads_by_sources",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_store_intelligence_sales_report_estimates_comparison_attributes_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_store_intelligence_sales_report_estimates_comparison_attributes_data"
    )(
        bq_table_name=f"raw_store_intelligence_sales_report_estimates_comparison_attributes",
        gcs_prefix=f"{GCS_PREFIX}/store_intelligence/top_apps/sales_report_estimates_comparision_attributes",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_app_intelligence_apps_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_app_intelligence_apps_data"
    )(
        bq_table_name=f"raw_app_intelligence_apps",
        gcs_prefix=f"{GCS_PREFIX}/app_intelligence/apps/apps",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_app_intelligence_get_reviews_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_app_intelligence_get_reviews_data"
    )(
        bq_table_name=f"raw_app_intelligence_get_reviews",
        gcs_prefix=f"{GCS_PREFIX}/app_intelligence/reviews/get_reviews",
        gcs_partition_expr=partition_expr
    )

    create_big_lake_table_ad_intelligence_get_creative_tops_data = create_big_lake_table_task.override(
        task_id="create_big_lake_table_ad_intelligence_get_creative_tops_data"
    )(
        bq_table_name=f"raw_ad_intelligence_creative_tops",
        gcs_prefix=f"{GCS_PREFIX}/ad_intelligence/top_creatives/creative_tops",
        gcs_partition_expr=partition_expr
    )

    """Group Of Tasks Based On EndPoints"""

    with TaskGroup(
        "get_usage_intelligence_demographics_data",
        tooltip="Get Usage Intelligence Demographics Data: https://app.sensortower.com/api/docs/usage_intel#/User%20Insights/app_analysis_demographics And Upload It To Big Query In 'raw_usage_intelligence_demographics' Table"
    ) as get_usage_intelligence_demographics_data:
        
        group_tasks_get_quaterly_demographic_data_in_app_ids(
            ds="{{ ds }}",
            group_task_id='group_tasks_get_quaterly_demographic_data_in_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )

        
        [
            group_tasks_get_quaterly_demographic_data_in_app_ids
        ]

    with TaskGroup(
        "get_usage_intelligence_retention_data",
        tooltip="Get Usage Intelligence Retention Data: https://app.sensortower.com/api/docs/usage_intel#/User%20Insights/app_analysis_retention And Upload It To Big Query In 'raw_usage_intelligence_retention' Table"
    ) as get_usage_intelligence_retention_data:        
        

        group_tasks_get_quaterly_retention_data_in_specific_app_ids(
            ds="{{ ds }}",
            group_task_id='group_tasks_get_quaterly_retention_data_in_specific_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )

        [ 
            group_tasks_get_quaterly_retention_data_in_specific_app_ids
        ]

    with TaskGroup(
        "get_usage_intelligence_app_overlap_data",
        tooltip="Get Usage Intelligence App Overlap Data: https://app.sensortower.com/api/docs/usage_intel#/User%20Insights/usage_app_overlap And Upload It To Big Query In 'raw_usage_intelligence_app_overlap' Table"
    ) as get_usage_intelligence_app_overlap_data:
        
        task_get_worldwide_sevenday_app_overlap_data_on_specific_app_ids(
            ds="{{ ds }}",
            task_id='task_get_worldwide_sevenday_app_overlap_data_on_specific_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID

        )

        [
            task_get_worldwide_sevenday_app_overlap_data_on_specific_app_ids
        ]
    
    
    with TaskGroup(
        "get_usage_intelligence_active_users_data",
        tooltip="Get Usage Intelligence App Overlap Data: https://app.sensortower.com/api/docs/usage_intel#/Usage%20Active%20Users/usage_active_users And Upload It To Big Query In 'raw_usage_intelligence_active_users' Table"
    ) as get_usage_intelligence_active_users_data:
        

       group_tasks_get_daily_and_monthly_active_users_data_on_specific_app_ids(
            ds="{{ ds }}",
            group_task_id='group_tasks_get_daily_and_monthly_active_users_data_on_specific_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
       )

       [
           group_tasks_get_daily_and_monthly_active_users_data_on_specific_app_ids
       ]

    with TaskGroup(
        "get_usage_intelligence_time_spent_data",
        tooltip="Get Usage Intelligence Time Spent Data: https://app.sensortower.com/api/docs/usage_intel#/Session%20Analysis/app_analysis_time_spent And Upload It To Big Query In 'raw_usage_intelligence_time_spent' Table"
    ) as get_usage_intelligence_time_spent_data:
        
        task_get_monthly_time_spent_data_on_specific_app_id(
            ds="{{ ds }}",
            task_id='task_get_monthly_time_spent_data_on_specific_app_id',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )
        

        [
            task_get_monthly_time_spent_data_on_specific_app_id
        ]

    with TaskGroup(
        "get_store_intelligence_sales_report_estimates_data",
        tooltip="Get Usage Intelligence Time Spent Data: https://app.sensortower.com/api/docs/usage_intel#/Session%20Analysis/app_analysis_time_spent And Upload It To Big Query In 'raw_usage_intelligence_time_spent' Table"
    ) as get_store_intelligence_sales_report_estimates_data:
        

        task_get_daily_sales_report_estimates_data_on_specific_app_ids(
            ds="{{ ds }}",
            task_id='task_get_daily_sales_report_estimates_data_on_specific_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )

        [
            task_get_daily_sales_report_estimates_data_on_specific_app_ids
        ]

    with TaskGroup(
        "get_store_intelligence_downloads_by_sources_data",
        tooltip="Get Usage Intelligence Time Spent Data: https://app.sensortower.com/api/docs/usage_intel#/Session%20Analysis/app_analysis_time_spent And Upload It To Big Query In 'raw_usage_intelligence_time_spent' Table"
    ) as get_store_intelligence_downloads_by_sources_data:
        
        task_get_download_by_source_all_countries_data_on_specific_app_ids(
            ds="{{ ds }}",
            task_id='task_get_download_by_source_all_countries_data_on_specific_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID

        )


        [
            task_get_download_by_source_all_countries_data_on_specific_app_ids
        ]

    with TaskGroup(
        "get_store_intelligence_sales_report_estimates_comparison_attributes_data",
        tooltip='Get Store Intelligence: Sales Report Estimate Comparision Data: https://app.sensortower.com/api/docs/store_intel#/Top%20Apps/top_and_trending and Upload It To Big Query In "raw_store_intelligence_sales_report_estimates_comparison_attributes" Table'
    ) as get_store_intelligence_sales_report_estimates_comparison_attributes_data:
        
        task_get_sales_report_estimates_comparision_attributes_on_specific_os_and_categories(
                ds="{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
                task_id='task_get_sales_report_estimates_comparision_attributes_on_specific_os_and_categories',
                gcs_bucket= BUCKET,
                gcs_prefix= GCS_PREFIX,
                http_conn_id=HTTP_CONN_ID
        )

        task_get_sales_report_estimates_comparision_attributes_on_unified_os_and_categories(
                ds="{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m-%d') }}",
                task_id='task_get_sales_report_estimates_comparision_attributes_on_unified_os_and_categories',
                gcs_bucket= BUCKET,
                gcs_prefix= GCS_PREFIX,
                http_conn_id=HTTP_CONN_ID
        )
        
        [
            task_get_sales_report_estimates_comparision_attributes_on_specific_os_and_categories,
            task_get_sales_report_estimates_comparision_attributes_on_unified_os_and_categories
        
        ]


    with TaskGroup(
        "get_app_intelligence_apps_data",
        tooltip='Get App Intelligence: Apps Data: https://app.sensortower.com/api/docs/app_intel#/Apps/apps and Upload It To Big Query In "raw_app_intelligence_apps" Table'
    ) as get_app_intelligence_apps_data:
        
        task_get_us_apps_data_with_app_ids_bq(
            ds="{{ ds }}",
            task_id='task_get_us_apps_data_with_app_ids_bq',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )

        [
            task_get_us_apps_data_with_app_ids_bq
        ]

    with TaskGroup(
        "get_app_intelligence_get_reviews_data",
        tooltip='Get App Intelligence: Get Reviews Data: https://app.sensortower.com/api/docs/app_intel#/Reviews/get_reviews and Upload It To Big Query In "raw_app_intelligence_get_reviews" Table'
    ) as get_app_intelligence_get_reviews_data:
        
        task_get_get_review_us_country_on_specific_app_ids(
            ds="{{ ds }}",
            task_id='task_get_get_review_us_country_on_specific_app_ids',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )

        [
            task_get_get_review_us_country_on_specific_app_ids
        ]
       

    with TaskGroup(
        "get_ad_intelligence_get_top_creatives_data",
        tooltip='Get Ad Intelligence: Get Top Creative Data: https://app.sensortower.com/api/docs/ad_intel#/Top%20Creatives/top_creatives and Upload it into "raw_ad_intelligence_creative_tops" table' 
    ) as get_ad_intelligence_get_top_creatives_data:
        
        group_tasks_get_creative_tops_on_specific_os_countries_and_categories(
            ds="{{ ds }}",
            group_task_id='group_tasks_get_creative_tops_on_specific_os_countries_and_categories',
            gcs_bucket= BUCKET,
            gcs_prefix= GCS_PREFIX,
            http_conn_id=HTTP_CONN_ID
        )

        [
            group_tasks_get_creative_tops_on_specific_os_countries_and_categories
        ]



"""App Intelligence"""

[ get_app_intelligence_get_reviews_data ] >> create_big_lake_table_app_intelligence_get_reviews_data


"""Ad Intelligence"""
[ get_ad_intelligence_get_top_creatives_data ] >> create_big_lake_table_ad_intelligence_get_creative_tops_data

"""Usage Intelligence"""
[ get_usage_intelligence_demographics_data ] >> create_big_lake_table_usage_intelligence_demographics_data

[ get_usage_intelligence_retention_data ] >> create_big_lake_table_usage_intelligence_retention_data

[ get_usage_intelligence_app_overlap_data ] >> create_big_lake_table_usage_intelligence_app_overlap_data

[ get_usage_intelligence_active_users_data ] >> create_big_lake_table_usage_intelligence_active_users_data

[ get_usage_intelligence_time_spent_data ] >> create_big_lake_table_usage_intelligence_time_spent_data



"""Store Intelligence"""
[ get_store_intelligence_sales_report_estimates_data ] >> create_big_lake_table_store_intelligence_sales_report_estimates_data

[ get_store_intelligence_downloads_by_sources_data ] >> create_big_lake_table_store_intelligence_downloads_by_sources_data

[ get_store_intelligence_sales_report_estimates_comparison_attributes_data ] >> create_big_lake_table_store_intelligence_sales_report_estimates_comparison_attributes_data




"""Other Dependency"""
create_big_lake_table_store_intelligence_sales_report_estimates_comparison_attributes_data >> [
    get_app_intelligence_apps_data
] >> create_big_lake_table_app_intelligence_apps_data

