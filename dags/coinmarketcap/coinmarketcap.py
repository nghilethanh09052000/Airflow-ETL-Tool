from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from coinmarketcap.scripts.coinmarketcap_currency_latest_listings import CoinMarketCapCurrency
from coinmarketcap.scripts.coinmarketcap_eth_quotes import CoinMarketCapETHQuotes
from coinmarketcap.scripts.coinmarketcap_eth_quotes_5m import CoinMarketCapETHQuotes5m
from coinmarketcap.scripts.coinmarketcap_main_tokens_daily_quotes import DailyMainTokenQuotes
from coinmarketcap.scripts.coinmarketcap_main_tokens_intraday_quotes import IntradayMainTokenQuotes
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

default_args = {
    "owner": "tri.nguyen",
    "start_date": datetime(2023, 2, 14),
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

schedule_interval = "0 1,6 * * *"

dag = DAG(
    "coinmarketcap",
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=2,
    tags=["coinmarketcap", "raw"],
    catchup=False,
)

coinmarketcap_raw = CoinMarketCapCurrency()
coinmarketcap_eth_quotes_raw = CoinMarketCapETHQuotes()
coinmarketcap_eth_quotes_5m = CoinMarketCapETHQuotes5m()
coinmarketcap_main_tokens_daily_quotes = DailyMainTokenQuotes()
coinmarketcap_main_tokens_intraday_quotes = IntradayMainTokenQuotes()

coinmarketcap_currency_latest_listings = PythonOperator(
    task_id="coinmarketcap_currency_latest_listings",
    dag=dag,
    provide_context=True,
    python_callable=coinmarketcap_raw.run,
)

coinmarketcap_eth_quotes_raw = PythonOperator(
    task_id="coinmarketcap_eth_quotes",
    dag=dag,
    provide_context=True,
    python_callable=coinmarketcap_eth_quotes_raw.run,
)

coinmarketcap_eth_quotes_5m = PythonOperator(
    task_id="coinmarketcap_eth_quotes_5m",
    dag=dag,
    provide_context=True,
    python_callable=coinmarketcap_eth_quotes_5m.run,
)

coinmarketcap_main_tokens_daily_quotes = PythonOperator(
    task_id="coinmarketcap_main_tokens_daily_quotes",
    dag=dag,
    provide_context=True,
    python_callable=coinmarketcap_main_tokens_daily_quotes.run,
)

coinmarketcap_main_tokens_intraday_quotes = PythonOperator(
    task_id="coinmarketcap_main_tokens_intraday_quotes",
    dag=dag,
    provide_context=True,
    python_callable=coinmarketcap_main_tokens_intraday_quotes.run,
)

coinmarketcap_eth_quote = BigQueryInsertJobOperator(
    task_id="coinmarketcap_eth_quote",
    gcp_conn_id="sipher_gcp",
    configuration={
        "query": {
            "query": "{% include 'query/coinmarketcap_eth_quote.sql' %}",  
            "useLegacySql": False
        }
    },
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

coinmarketcap_currency_latest_listings
coinmarketcap_eth_quotes_raw >> coinmarketcap_eth_quote
coinmarketcap_eth_quotes_5m
coinmarketcap_main_tokens_daily_quotes
coinmarketcap_main_tokens_intraday_quotes
