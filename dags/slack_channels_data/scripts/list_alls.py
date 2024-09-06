from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from typing import Sequence, Any, List
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from functools import wraps
from google.cloud import bigquery

import pandas as pd
import time
import logging
import uuid
import datetime

def slack_api_call_decorator(api_call_function):
    @wraps(api_call_function)
    def wrapper(self, *args, **kwargs):
        try:
            logging.info(f"Calling Slack API: {api_call_function.__name__}")
            response = api_call_function(self, *args, **kwargs)
            logging.info(f"Slack API call successful: {api_call_function.__name__}")
            return response
        except SlackApiError as e:
            logging.error(f"Slack API call failed: {e.response['error']}")
            raise e
    return wrapper


class SlackDataFetcher(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )
    
    def __init__(
        self,
        task_id: str,
        token: str,
        gcs_bucket: str,
        gcs_prefix: str,
        ds: str,
        **kwargs
    ):
        BaseOperator.__init__(self, task_id=task_id, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket=gcs_bucket, gcs_prefix=gcs_prefix, **kwargs)

        self.task_id = self.task_id
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.ds = ds
        self.client = WebClient(token=token)
    
    def execute(self, context: Any):
        raise NotImplementedError("Subclasses should implement this!")

    def _upload_data(self, data):
        collected_ts = round(time.time() * 1000)
        partition_prefix = f"snapshot_date={str(self.ds)}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )

class ListUsers(SlackDataFetcher):
    def __init__(self, team_id: str, **kwargs):
        super().__init__(**kwargs)
        self.team_id = team_id

    @slack_api_call_decorator
    def _get_users(self, team_id: str, next_cursor=None):
        items = self.client.users_list(cursor=next_cursor)
        members = items.get('members')

        for member in members:
            profile = member.get('profile')
            member_data = [{
                "id"                        : str(member.get('id')),
                "team_id"                   : str(team_id),
                "name"                      : str(member.get('name')),
                "deleted"                   : str(member.get('deleted')),
                "color"                     : str(member.get('color')),
                "real_name"                 : str(member.get('real_name')),
                "tz"                        : str(member.get('tz')),
                "tz_label"                  : str(member.get('tz_label')),
                "tz_offset"                 : str(member.get('tz_offset')),
                "title"                     : str(profile.get('title')),
                "phone"                     : str(profile.get('phone')),
                "skype"                     : str(profile.get('skype')),
                "email"                     : str(profile.get('email')),
                "is_admin"                  : str(member.get('is_admin')),
                "is_owner"                  : str(member.get('is_owner')),
                "is_primary_owner"          : str(member.get('is_primary_owner')),
                "is_restricted"             : str(member.get('is_restricted')),
                "is_ultra_restricted"       : str(member.get('is_ultra_restricted')),
                "is_bot"                    : str(member.get('is_bot')),
                "is_app_user"               : str(member.get('is_app_user')),
                "updated"                   : str(member.get('updated')),
                "is_email_confirmed"        : str(member.get('is_email_confirmed')),
                "who_can_share_contact_card": str(member.get('is_bot'))
            }]

            logging.info(f'Member Data Getting..................... {member_data}')
            self._upload_data(member_data)

        response_metadata = items.get('response_metadata')
        if response_metadata and response_metadata.get('next_cursor'):
            next_cursor = response_metadata.get('next_cursor')
            self._get_users(team_id=team_id, next_cursor=next_cursor)

    def execute(self, context: Any):
        self._get_users(self.team_id)

class ListChannels(SlackDataFetcher):
    @slack_api_call_decorator
    def _get_channels(self, next_cursor=None):
        items = self.client.conversations_list(cursor=next_cursor)
        channels = items.get('channels')

        for channel in channels:
            channel_data = [{
                "id"             : str(channel.get('id')),
                "name"           : str(channel.get('name')),
                "is_channel"     : str(channel.get('is_channel')),
                "is_group"       : str(channel.get('is_group')),
                "is_im"          : str(channel.get('is_im')),
                "created"        : str(channel.get('created')),
                "creator"        : str(channel.get('creator')),
                "is_archived"    : str(channel.get('is_archived')),
                "is_general"     : str(channel.get('is_general')),
                "name_normalized": str(channel.get('name_normalized')),
                "is_shared"      : str(channel.get('is_shared')),
                "is_org_shared"  : str(channel.get('is_org_shared')),
                "is_member"      : str(channel.get('is_member')),
                "is_private"     : str(channel.get('is_private')),
                "is_mpim"        : str(channel.get('is_mpim'))
            }]

            logging.info(f'Channel Data Getting..................... {channel_data}')
            self._upload_data(channel_data)

        response_metadata = items.get('response_metadata')
        if response_metadata and response_metadata.get('next_cursor'):
            next_cursor = response_metadata.get('next_cursor')
            self._get_channels(next_cursor=next_cursor)

    def execute(self, context: Any):
        self._get_channels()

class ListPermanentLinks(SlackDataFetcher):

    def __init__(
            self, 
            bq_project: str, 
            gcp_conn_id: str,
            **kwargs
        ):
        super().__init__(**kwargs)
        self.bq_project = bq_project
        self.gcp_conn_id = gcp_conn_id

    def _get_bigquery_results(self, next_cursor=None):
        service_account_json_path = BaseHook.get_connection(self.gcp_conn_id).extra_dejson["key_path"]
        bq_client = bigquery.Client.from_service_account_json(service_account_json_path)
        query = f""" 
            WITH
                ranked_messages AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY client_msg_id, channel_id, author, message ORDER BY __collected_ts DESC) AS row_num
                    FROM
                        `sipher-data-platform.slack_data.slack_messages`
                WHERE    
                    DATE(TIMESTAMP_SECONDS(CAST(SPLIT(created, '.')[OFFSET(0)] AS INT64))) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

                )
                SELECT DISTINCT
                    client_msg_id,
                    channel_id,
                    message,
                    created
                FROM
                    ranked_messages
                WHERE
                    row_num = 1
                ORDER BY
                    created 
        """
        query_job = bq_client.query(query, project=self.bq_project )
        df = query_job.to_dataframe()
        
        # df['created'] = df['created'].apply(lambda x: datetime.datetime.fromtimestamp(float(x)))
        # df['message_ts'] = df['created'].apply(lambda x: int(x.timestamp()) if pd.notnull(x) else None)

        results = df.to_dict(orient='records')

        return results

    @slack_api_call_decorator 
    def _get_slack_archive_link(self, result):
        try:
            item = self.client.chat_getPermalink(
                channel=result.get('channel_id'),
                message_ts=str(result.get('created'))
            )
            logging.info(f'RESULT-------------------------{item}')

            client_msg_id = result.get('client_msg_id')
            channel_id    = item.get('channel')
            created       = result.get('created')
            permalink     = item.get('permalink')
            
            data = {
                'client_msg_id': client_msg_id,
                'channel_id'   : channel_id,
                'created'      : created,
                'permalink'    : permalink
            }
            self._upload_data(data)

        except SlackApiError as e:
            logging.error(f"Slack API call failed: {e.response['error']}")
            if e.response['error'] == 'message_not_found':
                logging.warning("Message not found. Skipping...")
                return None
            else:
                raise e

        
    def execute(self, context: Any):
        results = self._get_bigquery_results()
        for result in results: self._get_slack_archive_link(result=result)
