from datetime import datetime
from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators
DAG_START_DATE = set_env_value(
    production=datetime(2024, 3, 14), dev=datetime(2024, 3, 14)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="0 2 * * *", dev="@once")
DEFAULT_ARGS = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}
DEFAULT_ARGS.update(default_args_for_dbt_operators)
with DAG(
    dag_id="sipher_odyssey_shop_transaction",
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["game", "sipher-odyssey", "shop"],
    catchup=False,
) as dag:

    stg_sipher_odyssey_shop_transaction = DbtRunOperator(
        task_id="stg_sipher_odyssey_shop_transaction",
        models="stg_sipher_odyssey_shop_transaction",
    )

    int_sipher_odyssey_transactions_sipher_google = DbtRunOperator(
        task_id="int_sipher_odyssey_transactions_sipher_google",
        models="int_sipher_odyssey_transactions_sipher_google",
    )

    int_sipher_odyssey_shop_transaction = DbtRunOperator(
        task_id="int_sipher_odyssey_shop_transaction",
        models="int_sipher_odyssey_shop_transaction",
    )

    fct_sipher_odyssey_shop_transaction = DbtRunOperator(
        task_id="fct_sipher_odyssey_shop_transaction",
        models="fct_sipher_odyssey_shop_transaction",
    )

    rpt_sipher_odyssey__new_user_profitability_ratio = DbtRunOperator(
        task_id="rpt_sipher_odyssey__new_user_profitability_ratio",
        models="rpt_sipher_odyssey__new_user_profitability_ratio",
    )

stg_sipher_odyssey_shop_transaction >> int_sipher_odyssey_transactions_sipher_google >> int_sipher_odyssey_shop_transaction >> fct_sipher_odyssey_shop_transaction >> rpt_sipher_odyssey__new_user_profitability_ratio
