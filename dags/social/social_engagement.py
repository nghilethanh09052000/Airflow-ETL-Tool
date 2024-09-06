from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from social.operators.facebook.page_feed import FacebookPageFeedOperator
from social.operators.facebook.page_insights import FacebookPageInsightsOperator
from social.operators.facebook.page_impression_gender_locate import FacebookPageImpressionOperator
from social.operators.facebook.page_overall import FacebookPageOverallOperator
from social.operators.facebook.post_insights import FacebookPostInsightsOperator
from social.operators.facebook.post_comments import FacebookPostCommentsOperator
from social.scripts.twitter import (
    TwitterTimeline,
    TwitterUserLookup
)
from social.scripts.tiktok import (
    TiktokVideo, 
    TiktokProfile
)


from social.operators.youtube.overviews_reports import YoutubeOverviewReportsOperator
from social.operators.youtube.traffic_sources import YoutubeTrafficSourcesOperator
from social.operators.youtube.content import YoutubeContentOperator
from social.operators.youtube.geographic_areas import YoutubeGeographicOperator
from social.operators.youtube.demographics import YoutubeDemographicsOperator
from social.scripts.facebook_utils import Pages, Params_fb, get_facebook_post_ids
from social.scripts.youtube_utils import Channels, Params_yt
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from utils.dbt import default_args_for_dbt_operators

from social.scripts.adhoc_social_followers import (
    post_followers_data
)


DAG_START_DATE = set_env_value(
    production=datetime(2023, 11, 1), dev=datetime(2023, 12, 25)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")

GCP_CONN_ID = "sipher_gcp"

BIGQUERY_PROJECT = Variable.get("bigquery_project")
BQ_DATASET = "raw_social"

BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
GCS_PREFIX = "social_data"

HTTP_CONN_ID = "facebook_page"
POST_IDs = "{{ ti.xcom_pull(key='post_ids') }}"

default_args = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
}
default_args.update(default_args_for_dbt_operators)

TWITTER_TOKEN = Variable.get(f"twitter_token", deserialize_json=True)


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


@task(task_id="get_post_ids")
def get_facebook_post_ids_task(**kwargs):
    post_ids = get_facebook_post_ids(
        gcp_conn_id=GCP_CONN_ID, bq_project=BIGQUERY_PROJECT, **kwargs
    )
    kwargs["ti"].xcom_push(key="post_ids", value=post_ids)


with DAG(
    dag_id="social_engagement",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    concurrency= 3,
    max_active_runs= 1,
    tags=["social", "bigquery"],
    catchup=False,
) as dag:
    

    

    with TaskGroup(group_id="twitter") as twitter:
        with TaskGroup(group_id="pull_data_user_lookup") as pull_data_user_lookup_group:
                
                user_name = "SIPHERxyz"
                
                gcs_prefix = f"{GCS_PREFIX}/twitter/user"
                bq_table_name = f"twitter_profile_stats__{user_name}__"
                dbt_stg_model_name = f"stg_twitter_profile_stats"
                partition_expr = "{account:STRING}/{snapshot_date:DATE}"

                create_big_lake_table = create_big_lake_table_task(
                    bq_table_name=bq_table_name,
                    gcs_prefix=gcs_prefix,
                    gcs_partition_expr=partition_expr,
                )

                populate_report_to_staging = DbtRunOperator(
                    task_id="populate_report_to_staging",
                    models=dbt_stg_model_name,
                    vars={"snapshot_date": "{{ ds }}"},
                )

                
                @task(task_id=f"twitter_user_lookup_sipherxyz")
                def twitter_user_lookup(user_name, **kwargs):
                    ds = kwargs.get("ds")
                    TwitterUserLookup.airflow_callable(
                        TWITTER_TOKEN.get(user_name),
                        ds=ds
                    )
    
                twitter_user_lookup(user_name) >> create_big_lake_table >> populate_report_to_staging

        # reports = ["timeline"]
        # for report in reports:
        #     with TaskGroup(group_id=report) as twitter_report_group:

        #         gcs_prefix = f"{GCS_PREFIX}/twitter/{report}"
        #         bq_table_name = f"twitter_{report}"
        #         dbt_stg_model_name = f"stg_twitter_{report}"
        #         partition_expr = "{account:STRING}/{snapshot_date:DATE}"

        #         create_big_lake_table = create_big_lake_table_task(
        #             bq_table_name=bq_table_name,
        #             gcs_prefix=gcs_prefix,
        #             gcs_partition_expr=partition_expr,
        #         )

        #         populate_report_to_staging = DbtRunOperator(
        #             task_id="populate_report_to_staging",
        #             models=dbt_stg_model_name,
        #             vars={"snapshot_date": "{{ ds }}"},
        #         )

        #         if report == "timeline":
        #             with TaskGroup(group_id=f"pull_data") as pull_data:
                            
        #                 user_name = "SIPHERxyz"

        #                 @task(task_id=f"{user_name}")
        #                 def twitter_timeline(user_name, **kwargs):
        #                     ds = kwargs.get("ds")
        #                     TwitterTimeline.airflow_callable(
        #                         account=TWITTER_TOKEN.get(user_name), ds=ds
        #                     ) 

        #                 twitter_timeline(user_name)

        #             rpt_twitter_post_stats = DbtRunOperator(
        #                 task_id="rpt_twitter_post_stats",
        #                 models="rpt_twitter_post_stats",
        #             )

        #             populate_report_to_staging >> rpt_twitter_post_stats

        #         pull_data >> create_big_lake_table >> populate_report_to_staging
                            
    with TaskGroup(group_id="tiktok") as tiktok:
        
        # with TaskGroup(group_id='tiktok_video_data') as tiktok_video_data:

        #     gcs_prefix = f"{GCS_PREFIX}/tiktok/video"
        #     bq_table_name = "tiktok_video"
        #     partition_expr = "{snapshot_date:DATE}"
            
        #     create_big_lake_table = create_big_lake_table_task(
        #             bq_table_name=bq_table_name,
        #             gcs_prefix=gcs_prefix,
        #             gcs_partition_expr=partition_expr,
        #         )

        #     get_tiktok_video = TiktokVideo(
        #         task_id = 'get_tiktok_video',
        #         gcs_bucket=BUCKET,
        #         gcs_prefix=f"{GCS_PREFIX}/tiktok/video",
        #         gcp_conn_id=GCP_CONN_ID,
        #         ds = "{{ ds }}"
        #     )

        #     get_tiktok_video >> create_big_lake_table

        
        with TaskGroup(group_id='tiktok_profile_data') as tiktok_profile_data:

            gcs_prefix = f"{GCS_PREFIX}/tiktok/profile"
            bq_table_name = "tiktok_profile"
            partition_expr = "{snapshot_date:DATE}"
            
            create_big_lake_table = create_big_lake_table_task(
                    bq_table_name=bq_table_name,
                    gcs_prefix=gcs_prefix,
                    gcs_partition_expr=partition_expr,
                )

            get_tiktok_profile = TiktokProfile(
                task_id = 'get_tiktok_profile',
                gcs_bucket=BUCKET,
                gcs_prefix=f"{GCS_PREFIX}/tiktok/profile",
                ds = "{{ ds }}"
            )

            get_tiktok_profile >> create_big_lake_table

    with TaskGroup(group_id="facebook_data") as facebook_group:
        with TaskGroup(group_id="fb_ingest") as facebook_ingest_group:
            facebook_post_ids = get_facebook_post_ids_task()

            report_types = Params_fb.REPORT_TYPES

            for report in report_types.keys():

                with TaskGroup(group_id=report) as facebook_report_type_group:
                    gcs_prefix = f"{GCS_PREFIX}/facebook/{report}"
                    bq_table_name = f"facebook_sipher_{report}"
                    dbt_stg_model_name = f"stg_facebook_{report}"

                    # =======Common operators =============
                    create_big_lake_table = create_big_lake_table_task(
                        bq_table_name=bq_table_name,
                        gcs_prefix=gcs_prefix,
                        gcs_partition_expr=report_types.get(report).get("partition_expr"),
                    )

                    populate_report_to_staging = DbtRunOperator(
                        task_id="populate_report_to_staging",
                        models=dbt_stg_model_name,
                        vars={"snapshot_date": "{{ ds }}"},
                    )
                    #=======================

                    # Specific operators and dependency based on report
                    if report == "page_overall":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                            for page in Pages:
                                FacebookPageOverallOperator(
                                    task_id=f"{page.name}_overall",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    http_conn_id=HTTP_CONN_ID,
                                    page_id=page.value,
                                    fields=Params_fb.PAGE_FIELDS,
                                )
                        pull_data

                    if report == "page_insights":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                            for page in Pages:
                                FacebookPageInsightsOperator(
                                    task_id=f"{page.name}_insights",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    http_conn_id=HTTP_CONN_ID,
                                    page_id=page.value,
                                    metrics=Params_fb.PAGE_INSIGHTS_METRICS,
                                )
                        pull_data

                    if report == "page_feed":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                            for page in Pages:
                                FacebookPageFeedOperator(
                                    task_id=f"{page.name}_feed",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    http_conn_id=HTTP_CONN_ID,
                                    page_id=page.value,
                                    fields=Params_fb.POST_FIELDS,
                                )
                        populate_report_to_staging >> facebook_post_ids

                    if report == "page_impression_gender_locate":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                            for page in Pages:
                                FacebookPageImpressionOperator(
                                    task_id=f"{page.name}_inmpression_gender_locate",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    http_conn_id=HTTP_CONN_ID,
                                    page_id=page.value,
                                    metrics=Params_fb.PAGE_IMPRESSION_GENDER_LOCATE,
                                )
                        pull_data

                    if report == "post_insights":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                            FacebookPostInsightsOperator(
                                task_id="post_insights",
                                gcs_bucket=BUCKET,
                                gcs_prefix=gcs_prefix,
                                http_conn_id=HTTP_CONN_ID,
                                post_ids=POST_IDs,
                                metrics=Params_fb.POST_METRICS,
                            )
                        facebook_post_ids >> pull_data

                    if report == "post_comments":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                            FacebookPostCommentsOperator(
                                task_id="post_comments",
                                gcs_bucket=BUCKET,
                                gcs_prefix=gcs_prefix,
                                http_conn_id=HTTP_CONN_ID,
                                post_ids=POST_IDs,
                            )
                        facebook_post_ids >> pull_data
                   #==========================
                    pull_data >> create_big_lake_table >> populate_report_to_staging
    
        with TaskGroup(group_id="fb_model") as facebook_model_group:

            dim_facebook_post = DbtRunOperator(
                task_id="dim_facebook_post",
                models= "dim_facebook_post",
            )

            fct_facebook_page = DbtRunOperator(
                task_id="fct_facebook_page",
                models= "fct_facebook_page",
            )

            fct_facebook_post_insights = DbtRunOperator(
                task_id="fct_facebook_post_insights",
                models= "fct_facebook_post_insights",
            )

            dim_facebook_post >> fct_facebook_post_insights
   
        facebook_ingest_group >> facebook_model_group

    with TaskGroup(group_id="youtube") as youtube:

        with TaskGroup(group_id="ingest") as youtube_ingest_group:
            report_types = Params_yt.REPORT_TYPES
            for report in report_types.keys():
                with TaskGroup(group_id=report) as facebook_report_type_group:
                    gcs_prefix = f"{GCS_PREFIX}/youtube/{report}"
                    bq_table_name = f"youtube_{report}"
                    dbt_stg_model_name = f"stg_youtube_{report}"

                    # =======Common operators =============
                    create_big_lake_table = create_big_lake_table_task(
                        bq_table_name=bq_table_name,
                        gcs_prefix=gcs_prefix,
                        gcs_partition_expr=report_types.get(report).get("partition_expr"),
                    )

                    populate_report_to_staging = DbtRunOperator(
                        task_id="populate_report_to_staging",
                        models=dbt_stg_model_name,
                        vars={"snapshot_date": "{{ ds }}"},
                    )
                    # =======================
                    if report == "overview":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeOverviewReportsOperator(
                                    task_id="overview",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.DIMENSIONS,
                                )
                        pull_data
                    
                    if report == "traffic_sources":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeTrafficSourcesOperator(
                                    task_id="traffic_sources",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.TRAFFIC_SOURCES,
                                )
                        pull_data
                    
                    if report == "contents":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeContentOperator(
                                    task_id="contents",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.CONTENTS,
                                    part= Params_yt.PART_CONTENT,
                                )
                        pull_data 
                    
                    if report == "geographic_areas":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeGeographicOperator(
                                    task_id="geographic_areas",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                    metrics= Params_yt.DIMENSIONS,
                                )
                        pull_data

                    if report == "demographics":
                        with TaskGroup(group_id=f"pull_data") as pull_data:
                                YoutubeDemographicsOperator(
                                    task_id="demographics",
                                    gcs_bucket=BUCKET,
                                    gcs_prefix=gcs_prefix,
                                    channel_id= Channels.SIPHER,
                                )
                        pull_data
                    
                pull_data >> create_big_lake_table >> populate_report_to_staging
        with TaskGroup(group_id="model") as youtube_model_group:

            dim_youtube_contents = DbtRunOperator(
                task_id="dim_youtube_contents",
                models= "dim_youtube_contents",
            )

            fct_youtube_channel = DbtRunOperator(
                task_id="fct_youtube_channel",
                models= "fct_youtube_channel",
            )

            fct_youtube_contents = DbtRunOperator(
                task_id="fct_youtube_contents",
                models= "fct_youtube_contents",
            )

        youtube_ingest_group  >> youtube_model_group 
       
    
    with TaskGroup(group_id="adhoc") as adhoc:
         
        get_social_followers = BigQueryInsertJobOperator(
            task_id="get_social_followers",
            dag=dag,
            configuration={
                "query": {
                    "query": "{% include 'query/adhoc_social_followers.sql' %}",  
                    "useLegacySql": False
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        post_social_followers = PythonOperator(
            task_id="post_social_followers",
            dag=dag,
            provide_context=True,
            python_callable=post_followers_data,
        )

        get_social_followers >> post_social_followers
