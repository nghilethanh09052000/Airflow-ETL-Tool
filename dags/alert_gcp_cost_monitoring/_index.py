from google.cloud import bigquery
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from typing import Union, Any, Dict
from slack_sdk import WebClient

import datetime
import logging


class BigQueryDataFetcher:
    def __init__(
        self, 
        gcp_conn_id, 
        project_id, 
        **kwargs
    ):
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id

    def get_data(self, query: str):
        
        logging.info(f"Query Processed: {query}")
        service_account_json_path = BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"]
        bq_client = bigquery.Client.from_service_account_json(service_account_json_path)
        query_job = bq_client.query(query, project=self.project_id)
        return query_job



class ConditionChecker:
    _table_name =  f"{Variable.get('bigquery_project')}.gcloud_monitoring.bigquery_query_usage"

    @staticmethod
    def _check_total_today_cost(bigquery_fetcher) -> Union[Dict[str, Any], None]:
        """
            Alert to check: Total cost today >= 100$
        """
        condition = 100
        message_text = f'*TOTAL COST TODAY IS GREATER THAN {condition}*'
        query = f"""
            SELECT 
                SUM(total_bytes_processed / (1024 * 1024 * 1024) * 0.005) AS total_cost_usd 
            FROM
               `{ConditionChecker._table_name}`
            WHERE 
                DATE(creation_time) = CURRENT_DATE()
            HAVING 
                total_cost_usd >= {condition};
        """

        results = bigquery_fetcher.get_data(query=query)
        data = results.result()
        
        data_list = list(data)

        if not data_list: return None, None

        row = data_list[0]
        total_cost_usd = row['total_cost_usd']

        info = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":small_red_triangle: {str(total_cost_usd)} :small_red_triangle: "
            }
        }
    
        return info, message_text

    @staticmethod
    def _check_total_today_cost_compare_one_week(bigquery_fetcher) -> Union[Dict[str, Any], None]:
        """
            Alert to check: Total Cost today >= AVG(7day cost before) * 3 (exceed_threshold)
        """
        condtion = 3
        message_text = ':small_red_triangle:  *TOTAL COST TODAY COMPARE TO AVERAGE 7 DAYS BEFORE* :small_red_triangle:'
        query = f"""
            WITH daily_costs AS (
                SELECT
                    DATE(creation_time) AS query_date,
                    SUM(total_bytes_processed / (1024 * 1024 * 1024) * 0.005) AS total_cost_usd
                FROM
                   `{ConditionChecker._table_name}`
                WHERE
                    DATE(creation_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
                GROUP BY
                    query_date
                ),
            avg_7day_cost AS (
                SELECT
                    AVG(total_cost_usd) AS avg_cost_usd
                FROM
                    daily_costs
                )
            ,today_cost AS (
                SELECT 
                    total_cost_usd  AS total_today_cost
                FROM 
                    daily_costs 
                WHERE 
                    query_date = CURRENT_DATE()
            )
                SELECT
                    total_today_cost,
                    avg_cost_usd AS avg_cost_last_7days,
                    total_today_cost >= (avg_cost_usd * {condtion}) AS exceed_threshold
                FROM
                    today_cost, avg_7day_cost
        
        """
        results = bigquery_fetcher.get_data(query=query)
        data = results.result()
        row = next(data)

        if not row: return None, None

        exceed_threshold = row['exceed_threshold']

        if not exceed_threshold: return None, None

        info = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Total Cost Today (Exceed threshold): `${row['total_today_cost']}`\n Average Cost Last 7 Days: `${row['avg_cost_last_7days']}`"
            }
        }

        return info, message_text

    @staticmethod
    def _check_total_today_cost_per_project_compare_one_week(bigquery_fetcher) -> Union[Dict[str, Any], None]:
        """
            Alert to check: Total Cost per project today >= AVG(7day cost before) * 3 (exceed_threshold)
        """
        condition = 3
        text = ''
        message_text = ':small_red_triangle:  *TOTAL COST PER PROJECT TODAY COMPARE TO AVERAGE 7 DAYS COST PER PROJECT BEFORE* :small_red_triangle:'
        query = f"""
            WITH project_daily_costs AS (
                SELECT
                    project_id,
                    DATE(creation_time) AS query_date,
                    SUM(total_bytes_processed / (1024 * 1024 * 1024) * 0.005) AS total_cost_usd
                FROM
                    `{ConditionChecker._table_name}`
                WHERE
                    DATE(creation_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
                GROUP BY
                    project_id, query_date
            ),
            project_avg_7day_costs AS (
                SELECT
                    project_id,
                    AVG(total_cost_usd) AS avg_cost_usd
                FROM
                    project_daily_costs
                WHERE
                    query_date < CURRENT_DATE()
                GROUP BY
                    project_id
            ),
            project_today_cost AS (
                SELECT 
                    project_id,
                    CAST(SUM(total_bytes_processed / (1024 * 1024 * 1024) * 0.005) AS FLOAT64) AS total_today_cost
                FROM 
                    `{ConditionChecker._table_name}`
                WHERE 
                    DATE(creation_time) = CURRENT_DATE()
                GROUP BY 
                    project_id
            )
            ,results AS (
                SELECT
                    today.project_id,
                    today.total_today_cost AS total_today_cost,
                    avg_7day.avg_cost_usd AS avg_cost_last_7days,
                    today.total_today_cost >= (avg_7day.avg_cost_usd * {condition}) AS exceed_threshold
                FROM
                    project_today_cost today
                JOIN
                    project_avg_7day_costs avg_7day ON today.project_id = avg_7day.project_id
            )

            SELECT 
                project_id,
                total_today_cost,
                avg_cost_last_7days
            FROM 
                results
            WHERE 
                total_today_cost > 0
                AND avg_cost_last_7days > 0
                AND exceed_threshold = true
        """
        results = bigquery_fetcher.get_data(query=query)
        
        rows = [row for row in results]
        if not rows: return None, None

        for row in rows:
            project_id = row['project_id']
            total_today_cost = row['total_today_cost']
            avg_cost_last_7days = row['avg_cost_last_7days']

            text += f"\n:fire: Project Id: *{project_id}*\n ::money_with_wings: Total Today Cost: `{total_today_cost}`\n :heavy_dollar_sign: Average Cost 7 Days: `{avg_cost_last_7days}`\n  \n"

        info = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        }

        return info, message_text
     
    @staticmethod
    def _check_query_cost_compute_greater_than_five(bigquery_fetcher) -> Union[Dict[str, Any], None]:
        """
            Alert that check: Query that >= $5 on today
        """
        condition = 5
        message_text = f'*USER QUERY THAT COST GREATER THAN {condition} DOLAR*'
        text = ''
        query = f"""
            SELECT DISTINCT
                user_email,
                query,
                job_id,
                project_id,
                total_bytes_processed / (1024 * 1024 * 1024) * 0.005 AS cost_usd
            FROM
               `{ConditionChecker._table_name}`
            WHERE 
                total_bytes_processed / (1024 * 1024 * 1024) * 0.005 >= {condition}
                AND
                DATE(creation_time) = CURRENT_DATE()
            ;
        """
        results = bigquery_fetcher.get_data(query=query)

        rows = [row for row in results]

        if not rows: return None, None
      
        for row in rows:

            user_email = row['user_email']
            cost_usd = row['cost_usd']
            job_id = row['job_id']
            project_id = row['project_id']
            text += f"\n:pepepray: Project Id: *{project_id}*\n :email: User Email: {user_email}\n :banjo: Job Id: `{job_id}`\n :bank: Cost: $_{cost_usd}_\n \n"

        info = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        }

        return info, message_text
    
    @staticmethod
    def _check_top_five_user_most_compute_query(bigquery_fetcher) -> Union[Dict[str, Any], None]:
        """
            Alert to checkL Top 5 users have highest cost from the last 7 days to today
        """
        text = ''
        message_text = f'*TOP 5 USERS HAVE HIGHEST COST IN QUERY COMPARE TO TODAY OVER THE LAST 7 DAYS*'
        query = f"""
            SELECT
                DISTINCT user_email,
                total_bytes_processed / (1024 * 1024 * 1024) * 0.005 AS cost_usd,
                creation_time,
                job_id,
                project_id
            FROM
                `{ConditionChecker._table_name}`
            WHERE
                DATE(creation_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
                ORDER BY 
                    cost_usd DESC
            LIMIT 5
        """

        results = bigquery_fetcher.get_data(query=query)

        rows = [row for row in results]

        if not rows: return None, None

        for row in rows:

            user_email = row['user_email']
            cost_usd = row['cost_usd']
            creation_time = row['creation_time']
            job_id = row['job_id']
            project_id = row['project_id']

            text += f"\n:proud: Project Id: *{project_id}*\n :email: User Email: {user_email}\n :fire: Job Id: `{job_id}`\n :moneybag: Cost: $_{cost_usd}_\n :clock8: Creation Time: {creation_time}\n  \n"

        info = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        }

        return info, message_text
    


class AlertingService:

    _dashboard_url = 'https://lookerstudio.google.com/u/3/reporting/aed8f7fc-547b-4218-91e4-8c2224b1de7a/page/tEnnC'
    _channel = 'C071WKY0KQW'
    _slack_token = Variable.get('slack_sipher_odyssey_alerting_token')
    _users = ', '.join([ f"@{name}" for name in Variable.get('gcp_cost_monitoring_alerting_user_abnormal').split(', ')])

    @staticmethod
    def _format_blocks_data(message_block, message_text):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":alert: Google Cloud BigQuery Usage Alert :alert:"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Please Check*: {AlertingService._users}",
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message_text
                }
            },
            message_block,
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Looker Dashboard"},
                        "style": "primary",
                        "url": AlertingService._dashboard_url,
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "*Data provided by Sipher Bot*"
                    },
                    {
                        "type": "image",
                        "image_url": "https://cdn-icons-png.flaticon.com/512/9299/9299874.png",
                        "alt_text": "icon"
                    },
                    {
                        "type": "plain_text",
                        "emoji": True,
                        "text": current_date
                    }
                ]
            }
        ]
        return blocks

    @staticmethod
    def send_alert(message_block, message_text):

        client = WebClient(token=AlertingService._slack_token)

        blocks = AlertingService._format_blocks_data(message_block, message_text)

        client.chat_postMessage(
            channel=AlertingService._channel, 
            text=message_text, 
            blocks=blocks
        )


class BigQueryUsageAlerting(BaseOperator):
    def __init__(
            self, 
            task_id,
            gcp_conn_id, 
            project_id, 
            ds,
            **kwargs
        ):

        BaseOperator.__init__(self, task_id=task_id, **kwargs)
        self.task_id = self.task_id
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.ds = ds
        



    def execute(self, context):
        bigquery_fetcher = BigQueryDataFetcher(self.gcp_conn_id, self.project_id)
        method_name = "_" + self.task_id

        if not hasattr(ConditionChecker, method_name): raise ValueError(f"Method {method_name} not found in ConditionChecker")


        condition_check_func = getattr(ConditionChecker, method_name)
        task_message_block, message_text = condition_check_func(bigquery_fetcher)        

        if not task_message_block:
            logging.info('No Data That Match Condition, Skip Task....')
            return

        AlertingService.send_alert(task_message_block, message_text)
  
            

