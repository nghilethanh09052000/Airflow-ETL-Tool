from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from google.cloud import bigquery
from slack_sdk import WebClient
from google.oauth2 import service_account
import logging

CHANNEL_ID = 'C078KAC8GJJ'
USER = ', '.join([f"@{name}" for name in Variable.get('fast_forex_alerting_user_abnormal').split(', ')])

@task(task_id='compare_and_alert_exchange_rates')
def compare_and_alert_exchange_rates(
    project_id,
    gcp_conn_id, 
    ds=None, 
    **kwargs
):
    slack_token = Variable.get('slack_sipher_odyssey_alerting_token')
    client = WebClient(token=slack_token)
    credentials = service_account.Credentials.from_service_account_file(
        BaseHook.get_connection(gcp_conn_id).extra_dejson["key_path"],
    )
    bq_client = bigquery.Client(
        credentials=credentials,
        project=project_id
    )
    
    latest_table = f"`sipher-data-platform.fast_forex.exchange_rate`"
    base_table = f"`sipher-data-platform.fast_forex.base_exchange_rate`"
    
    latest_query = f"""
        SELECT date, currency, value, snapshot_date
        FROM {latest_table}
        WHERE snapshot_date = '{ds}'
    """
    
    # Query to fetch data from base table
    base_query = f"""
        SELECT currency, value, date
        FROM {base_table}
    """
    
    # Execute the queries
    latest_results = bq_client.query(latest_query).result()
    base_results = bq_client.query(base_query).result()
    
    latest_rates = {row['currency']: {'value': float(row['value']), 'date': row['date'] , 'snapshot_date': row['snapshot_date']} for row in latest_results}
    base_rates = {row['currency']: {'value': float(row['value']), 'date': row['date']} for row in base_results}
    
    alerts = []
    
    for currency, latest_data in latest_rates.items():
        
        latest_value = float(latest_data['value'])
        latest_date = latest_data['date']
        latest_snapshot_date = latest_data['snapshot_date']
        
        base_data = base_rates.get(currency)
        
        if base_data:
            base_value = float(base_data['value'])
            base_snapshot_date = base_data['date']
            
            percentage_change = ((latest_value - base_value) / base_value) * 100
            if abs(percentage_change) >= 5:
                alert_message = (f"Currency `{currency}` has changed by {percentage_change:.2f}% "
                                 f"(Base Value: `{base_value}`, Latest Value: `{latest_value}` )")
                alerts.append(alert_message)
                
                blocks = [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": ":alert: Currency Change Alerting :alert:"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Please Check*: {USER}",
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Alert*: {alert_message}"
                        }
                    },
                    {"type": "divider"},
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f":point_right: Base Date: `{base_snapshot_date}`\n"
                                    f":point_right: Latest Snapshot Date: `{latest_snapshot_date}`"
                        }
                    },
                    {"type": "divider"},
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": "*Data provided by Sipher Odyssey*"
                            }
                        ]
                    }
                ]
                
                client.chat_postMessage(channel=CHANNEL_ID, blocks=blocks)
                
                update_query = f"""
                    UPDATE {base_table}
                    SET 
                        based_value = '{str(base_value)}',
                        value = '{str(latest_value)}', 
                        date = '{str(latest_date)}',
                        exchange_rate = '{str(percentage_change)}'
                    WHERE 
                        currency = '{currency}'
                """
                query_job = bq_client.query(update_query)
                logging.info(f'DATA UPDATED----- {query_job}')
    
    return alerts
