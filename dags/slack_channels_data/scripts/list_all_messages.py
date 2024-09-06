from airflow.models.baseoperator import BaseOperator
from typing import List, Sequence, Any
from airflow.hooks.base import BaseHook
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from google.cloud import bigquery
import time
import datetime
import logging
import uuid
import re


class ListAllMessages(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )
    
    def __init__(
        self, 
        task_id: str,
        channel_ids: List[str],
        token: str,
        gcs_bucket: str,
        gcs_prefix: str,
        project_id: str,
        gcp_conn_id: str,
        ds: str,
        **kwargs
    ):
        BaseOperator.__init__(self, task_id=task_id, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket=gcs_bucket, gcs_prefix=gcs_prefix, **kwargs)

        self.task_id = self.task_id
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.ds = ds
        self.channel_ids = channel_ids
        self.client = WebClient(token=token)
        self.users = None
    

    def execute(self, context: Any):
        self.users = self._get_users()
        self._get_messages(self.channel_ids)

    def _get_users(self):
        service_account_json_path = BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"]
        bq_client = bigquery.Client.from_service_account_json(service_account_json_path)
        query = f"""SELECT DISTINCT id, name FROM `{self.project_id}.slack_data.slack_users` """
        query_job = bq_client.query(query, project=self.project_id)
        results = query_job.result()
    
        user_data = []
        for row in results:
            user_data.append({'id': row['id'], 'name': row['name']})
        
        return user_data
    
    def _get_messages(self, channel_ids: List[str], next_cursor=None):
        for channel_id in channel_ids:
            try:
                items = self.client.conversations_history(
                    channel=channel_id,
                    cursor=next_cursor
                )
                messages = [item for item in items.get('messages') if item.get('type') == 'message']

                for message in messages:
                    data = {
                        'client_msg_id': str(message.get('client_msg_id')),
                        'channel_id'   : str(channel_id),
                        'author'       : str(message.get('user')),
                        'message'      : self._format_message(str(message.get('text'))),
                        'urls'         : self._format_message_url(message.get('text')),
                        'tags'         : self._format_message_tag(message.get('text')),
                        'created'      : self._format_date_time(message.get('ts'))
                    }
                    
                    logging.info(f'Data Getting..................... {data}')
                    self._upload_data(data)

                has_more = items.get('has_more')
                if has_more:
                    next_cursor = items['response_metadata']['next_cursor']
                    self._get_messages(channel_ids=channel_ids, next_cursor=next_cursor)
            except SlackApiError as e:
                logging.error(f"Error retrieving messages: {e.response['error']}")
                break

    def _format_message(self, text: str) -> str:
        tag_regex = r'<@([^>\s|]+)>'
        matches = re.findall(tag_regex, text)
        
        # Replace user IDs with names
        for user_id in matches:
            user_name = next((user['name'] for user in self.users if user['id'] == user_id), f'UNKNOWN_USER_{user_id}')
            text = text.replace(f'<@{user_id}>', f'@{user_name}')
        
        return text

    def _format_message_url(self, text: str) -> str:
        url_regex = r'<(https?://\S+?)>'
        matches = re.findall(url_regex, text)
        if not matches: return ''
        return '\n'.join(matches)
    
    def _format_message_tag(self, text: str) -> str:
        tag_regex = r'<@([^>\s|]+)>'
        matches = re.findall(tag_regex, text)
        if not matches: return ''
        return '\n'.join(matches)
    
    def _format_date_time(self, timestamp_str: str) -> str:
        # timestamp = float(timestamp_str)
        # datetime_obj = datetime.datetime.fromtimestamp(timestamp)
        return str(timestamp_str)

    def _upload_data(self, data):
        collected_ts = round(time.time() * 1000)
        partition_prefix = f"snapshot_date={str(self.ds)}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
