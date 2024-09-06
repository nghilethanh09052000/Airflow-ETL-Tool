import uuid
import time
from typing import Any, Sequence, Dict, List
from airflow.models.operator import BaseOperator
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
import requests
import logging
import json
from lxml import html



class TiktokProfile(
    BaseOperator,
    GCSDataUpload
):
    
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )
    def __init__(
        self,
        task_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        ds: str,
        **kwargs
    ):

        BaseOperator.__init__(
            self, 
            task_id = task_id,
            **kwargs
        )

        GCSDataUpload.__init__(
            self, 
            gcs_bucket = gcs_bucket,
            gcs_prefix = gcs_prefix, 
            **kwargs
        )
        
        self.ds = ds

    def execute(self, context: Any):
       data = self._get_data()
       self._upload_data(data=data)

    def _get_data(self):
        response = requests.get(
            url='https://www.tiktok.com/@playsipher',
            headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',                
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
            }
        )
        html_response = response.content
        tree = html.fromstring(html_response)

        script = tree.xpath('//script[@id="__UNIVERSAL_DATA_FOR_REHYDRATION__"]/text()')[0]
        
        if not script:
            raise ValueError("Invalid Session Failed This Airflow Task")
        
        data = json.loads(script)
        user_info = data \
                    .get('__DEFAULT_SCOPE__') \
                    .get('webapp.user-detail') \
                    .get('userInfo')
        
        user = user_info.get('user')
        stats = user_info.get('stats')

        return {
            'id': str(user.get('id')),
            'unique_id': str(user.get('uniqueId')),
            'nickname': str(user.get('nickname')),
            'following_visibility' : str(user.get('followingVisibility')),
            'follower_count': str(stats.get('followerCount')),
            'following_count' : str(stats.get('followingCount')),
            'heart': str(stats.get('heart')),
            'heart_count': str(stats.get('heartCount')),
            'video_count': str(stats.get('videoCount')),
            'digg_count': str(stats.get('diggCount')),
            'friend_count': str(stats.get('friendCount'))
        }

    
    def _upload_data(self, data):
        collected_ts = round(time.time() * 1000)
        current_date = time.strftime("%Y-%m-%d")
        current_hour = time.strftime("%H")
        partition_prefix = f"snapshot_date={self.ds}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )