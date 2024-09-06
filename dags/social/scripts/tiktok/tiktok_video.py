import uuid
import time
import requests
import logging
import re
from utils.common import set_env_value
from typing import Any, Sequence, Dict, List
from airflow.models import Variable, BaseOperator
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from tenacity import retry, stop_after_attempt, wait_exponential

"""
    (Deprecated) Spreadsheet: https://docs.google.com/spreadsheets/d/19mPUpj1kmUynbKQCU8YMQIr3d7RkgnGmNIfoEV58nDU/edit#gid=0
    Titok : https://www.tiktok.com/@playsipher/
    Please check the Tiktok Channel and append new video id and url on the spreadsheet before upload
    
    (Updated 2024-06-03): API is missing, so we don't use it anymore, some fields of data will be leave at ''. 
    We just have data in stats, desc, creatime
"""

class TiktokVideo(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )

    def __init__(self, task_id: str, gcs_bucket: str, gcs_prefix: str, gcp_conn_id: str, ds: str, **kwargs):
        BaseOperator.__init__(self, task_id=task_id, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket=gcs_bucket, gcs_prefix=gcs_prefix, **kwargs)
        self.ds = ds

    def execute(self, context: Any):
        videos: List[Dict[str, str]] = self._get_init_website()
        for video in videos:
            self._get_data(video)
        logging.info('Uploading TikTok Video Successfully')

    @retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _get_init_website(self) -> List[Dict[str, str]]:
        url = 'https://www.tiktok.com/@playsipher'
        post_body = {"cmd": "request.get", "url": url, "maxTimeout": 60000}

        response = requests.post(
            set_env_value(
                production=f"http://34.136.51.127:8191/v1",
                dev='http://flaresolverr:8191/v1'
            ),
            json=post_body,
            headers={'Content-Type': 'application/json'}
        )
        data = response.json()
        if data.get('status') != 'ok':
            raise Exception(f"Failed to get response from {url}")

        html_response = data.get('solution').get('response')
        logging.info(f"Html Response-----------{html_response}")
        urls = re.findall(r"https://www\.tiktok\.com/@playsipher/video/\d+", html_response)
        if not urls:
            raise ValueError('URLs Cannot Be 0, Mark Task As Failed')
        return urls

    def _get_data(self, video: str):
        response = requests.get(
            url=video,
            headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0'
            }
        )
        text = response.text

        stats_pattern = re.compile(
            r'"diggCount":\s*(\d+),\s*"shareCount":\s*(\d+),\s*"commentCount":\s*(\d+),\s*"playCount":\s*(\d+),\s*"collectCount":\s*"?(\d+)"?'
        )
        webid_pattern = re.compile(r'"webIdCreatedTime":\s*"(\d+)"')
        desc_pattern = re.compile(r'"desc":\s*"([^"]+)"')

        stats_match = stats_pattern.search(text)
        webid_match = webid_pattern.search(text)
        desc_match = desc_pattern.search(text)

        if stats_match and webid_match and desc_match:
            data = {
                'video_id': video.split('/')[-1],
                'url': video,
                'create_time': webid_match.group(1),
                'desc': desc_match.group(1),
                'user_digged': '0',
                'rate': '',
                'is_top': '',
                'duration': '',
                'comment_count': stats_match.group(3),
                'digg_count': stats_match.group(1),
                'download_count': '',
                'play_count': stats_match.group(4),
                'share_count': stats_match.group(2),
                'forward_count': '',
                'lose_count': '',
                'lose_comment_count': '',
                'whatsapp_share_count': '',
                'collect_count': stats_match.group(5)
            }
            logging.info(f'Data Retrieved Successfully: {data}')
            self._upload_data(data)
        else:
            logging.warning(f"Statistics or description not found for video: {video}")

    def _upload_data(self, data: Dict[str, Any]):
        collected_ts = round(time.time() * 1000)
        partition_prefix = f"snapshot_date={self.ds}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
