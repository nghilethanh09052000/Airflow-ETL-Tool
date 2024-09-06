from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator

from etherscan.scripts.etherscan_polygon_important_wallet_accounts_balance import EtherscanWalletBalance
from utils.alerting.airflow import airflow_callback
from utils.dbt import default_args_for_dbt_operators

EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

default_args = {
    "owner": "tri.nguyen",
    "start_date": datetime(2023, 4, 12),
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

default_args.update(default_args_for_dbt_operators)

schedule_interval = "0 2 * * *"

dag = DAG(
    "etherscan",
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=2,
    tags=["etherscan", "raw"],
    catchup=False,
)

etherscan_polygon_important_wallet_accounts_balance = EtherscanWalletBalance()

etherscan_polygon_important_wallet_accounts_balance = PythonOperator(
    task_id="etherscan_polygon_important_wallet_accounts_balance",
    dag=dag,
    provide_context=True,
    python_callable=etherscan_polygon_important_wallet_accounts_balance.run,
)

stg_coinmarketcap__main_token_quotes =  DbtRunOperator(
    task_id="stg_coinmarketcap__main_token_quotes", 
    models="stg_coinmarketcap__main_token_quotes", 
    dag=dag
)

stg_coinmarketcap__main_token_quotes_intraday =  DbtRunOperator(
    task_id="stg_coinmarketcap__main_token_quotes_intraday", 
    models="stg_coinmarketcap__main_token_quotes_intraday", 
    dag=dag
)

# fct_important_wallets_transactions =  DbtRunOperator(
#     task_id="fct_important_wallets_transactions", 
#     models="fct_important_wallets_transactions", 
#     dag=dag
# )

# fct_important_wallets_token_transfers =  DbtRunOperator(
#     task_id="fct_important_wallets_token_transfers", 
#     models="fct_important_wallets_token_transfers", 
#     dag=dag
# )

mart_important_wallet_accounts_balance =  DbtRunOperator(
    task_id="mart_important_wallet_accounts_balance", 
    models="mart_important_wallet_accounts_balance", 
    dag=dag
)

rpt_finance_wallet_account_balance =  DbtRunOperator(
    task_id="rpt_finance_wallet_account_balance", 
    models="rpt_finance_wallet_account_balance", 
    dag=dag
)

etherscan_polygon_important_wallet_accounts_balance
[stg_coinmarketcap__main_token_quotes, stg_coinmarketcap__main_token_quotes_intraday] >> mart_important_wallet_accounts_balance
[stg_coinmarketcap__main_token_quotes, stg_coinmarketcap__main_token_quotes_intraday] >> rpt_finance_wallet_account_balance