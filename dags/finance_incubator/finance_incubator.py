from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators
from airflow.utils.task_group import TaskGroup

DAG_START_DATE = set_env_value(
    production=datetime(2024, 1, 12), dev=datetime(2024, 1, 12)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")

DEFAULT_ARGS = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_success",
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)

with DAG(
    dag_id="finance_incubator",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["revenue", "cost", "adjust", "appsflyer", "max_mediation"],
    catchup=False,
) as dag:

    with TaskGroup(group_id="staging", prefix_group_id=False) as staging:

        """staging adjust"""
        stg_adjust = DbtRunOperator(
            task_id="stg_adjust",
            models="stg_adjust",
        )
        """staging appsflyer"""
        stg_appsflyer__attributed_ad_revenue = DbtRunOperator(
            task_id="stg_appsflyer__attributed_ad_revenue",
            models="stg_appsflyer__attributed_ad_revenue",
        )

        stg_appsflyer__cohort_unified = DbtRunOperator(
            task_id="stg_appsflyer__cohort_unified",
            models="stg_appsflyer__cohort_unified",
        )

        stg_appsflyer__inapps = DbtRunOperator(
            task_id="stg_appsflyer__inapps",
            models="stg_appsflyer__inapps",
        )

        stg_appsflyer__installs = DbtRunOperator(
            task_id="stg_appsflyer__installs",
            models="stg_appsflyer__installs",
        )

        stg_appsflyer__organic_ad_revenue = DbtRunOperator(
            task_id="stg_appsflyer__organic_ad_revenue",
            models="stg_appsflyer__organic_ad_revenue",
        )
        """staging max mediation"""
        stg_max__ad_cohort_revenue = DbtRunOperator(
            task_id="stg_max__ad_cohort_revenue",
            models="stg_max__ad_cohort_revenue",
        )

        stg_max__ad_revenue = DbtRunOperator(
            task_id="stg_max__ad_revenue",
            models="stg_max__ad_revenue",
        )

        stg_max__user_ad_revenue = DbtRunOperator(
            task_id="stg_max__user_ad_revenue",
            models="stg_max__user_ad_revenue",
        )
    
    with TaskGroup(group_id="dim_fact", prefix_group_id=False) as dim_fact:

        """fact adjust"""
        dim_adjust_users = DbtRunOperator(
            task_id="dim_adjust_users",
            models="dim_adjust_users",
        )
        
        fct_adjust = DbtRunOperator(
            task_id="fct_adjust",
            models="fct_adjust",
        )
        """appsflyer, max mediation"""

        fct_fin_incubator_cost = DbtRunOperator(
            task_id="fct_fin_incubator_cost",
            models="fct_fin_incubator_cost",
        )

        fct_fin_incubator_revenue = DbtRunOperator(
            task_id="fct_fin_incubator_revenue",
            models="fct_fin_incubator_revenue",
        )

        dim_fin_sipher_version = DbtRunOperator(
            task_id="dim_fin_sipher_version",
            models="dim_fin_sipher_version",
        )

        in_fin_sipher_installs = DbtRunOperator(
            task_id="in_fin_sipher_installs",
            models="in_fin_sipher_installs",
        )

    with TaskGroup(group_id="mart_rpt", prefix_group_id=False) as mart_rpt:
       
        hidden_atlas_iap = DbtRunOperator(
            task_id="hidden_atlas_iap",
            models="hidden_atlas_iap",
        )

        rpt_adjust_campaign = DbtRunOperator(
            task_id="rpt_adjust_campaign",
            models="rpt_adjust_campaign",
        )
        
        rpt_adjust_channel = DbtRunOperator(
            task_id="rpt_adjust_channel",
            models="rpt_adjust_channel",
        )

        rpt_adjust_country = DbtRunOperator(
            task_id="rpt_adjust_country",
            models="rpt_adjust_country",
        )

        rpt_adjust_events = DbtRunOperator(
            task_id="rpt_adjust_events",
            models="rpt_adjust_events",
        )

        mart_fin_incubator_cost_revenue_iap_ads = DbtRunOperator(
            task_id="mart_fin_incubator_cost_revenue_iap_ads",
            models="mart_fin_incubator_cost_revenue_iap_ads",
        )
    
    with TaskGroup(group_id="ua_dashboard", prefix_group_id=False) as ua_dashboard:

        rpt_fin_incubator_roas = DbtRunOperator(
            task_id="rpt_fin_incubator_roas",
            models="rpt_fin_incubator_roas",
        )

        rpt_fin_incubator_cohort = DbtRunOperator(
            task_id="rpt_fin_incubator_cohort",
            models="rpt_fin_incubator_cohort",
        )

        rpt_fin_incubator_country_appsflyer = DbtRunOperator(
            task_id="rpt_fin_incubator_country_appsflyer",
            models="rpt_fin_incubator_country_appsflyer",
        )

        rpt_fin_roi_sipher = DbtRunOperator(
            task_id="rpt_fin_roi_sipher",
            models="rpt_fin_roi_sipher",
        )
        rpt_fin_incubator_roas >> rpt_fin_incubator_country_appsflyer
        rpt_fin_incubator_roas >> rpt_fin_roi_sipher
        

    staging >> dim_fact >> mart_rpt >> ua_dashboard
