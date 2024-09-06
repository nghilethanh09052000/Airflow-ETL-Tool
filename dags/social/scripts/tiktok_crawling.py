import logging
from datetime import datetime

import pandas as pd
import pandas_gbq as gbq
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.oauth2 import service_account

EXECUTION_ENVIRONMENT = Variable.get("execution_environment", default_var="dev")


class TiktokCrawler:
    def __init__(self):
        # tiktok info
        access = Variable.get(f"tiktok_token", deserialize_json=True)
        self.tiktok_info = {
            "token": access.get("token"),
            "username": access.get("username"),
        }

        self.urls = {
            "profile_stats": "https://www.influencerhunters.com/apis/tt/user/info",
            "video_stats": "https://www.influencerhunters.com/apis/tt/user/posts-from-secuid",
            "video_comments": "https://www.influencerhunters.com/apis/tt/post/comments",
        }

        # bigquery stuffs
        self.bigquery_project = "sipher-data-platform"
        self.bigquery_dataset = (
            "raw_social" if EXECUTION_ENVIRONMENT == "production" else "tmp3"
        )
        self.table_name = {
            "video_stats": f'tiktok_video_{datetime.today().strftime("%Y%m%d")}',
            "profile_stats": "tiktok_profile_stats",
            "user_info": "tiktok_user_info",
        }
        self.service_account_json_path = BaseHook.get_connection(
            "sipher_gcp"
        ).extra_dejson["key_path"]
        self.credentials = service_account.Credentials.from_service_account_file(
            self.service_account_json_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    def run(self):
        self.get_user_info()

    def get_request(self, url, params):
        try:
            logging.info(f"-- Getting request of {url}")
            response = requests.get(url, params=params)
            result = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error: {e}. Response text: {response.text}")

        return result

    def get_profile_stats(self):
        # get request for profile stats
        params = {
            "token": self.tiktok_info["token"],
            "username": self.tiktok_info["username"],
        }
        profile_stats_data = self.get_request(
            self.urls["profile_stats"], params=params
        )["data"]

        # setup dataframe
        columns = ["followers", "hearts", "diggs", "videos"]
        follower = profile_stats_data["stats"]["followerCount"]
        heart = profile_stats_data["stats"]["heartCount"]
        video = profile_stats_data["stats"]["videoCount"]
        digg = profile_stats_data["stats"]["diggCount"]
        profile_df = pd.DataFrame(
            columns=columns, data=[[follower, heart, digg, video]]
        )
        profile_df["date"] = datetime.today().strftime("%Y%m%d")

        # upload to BQ
        self.upload_to_bigquery(
            profile_df, self.table_name["profile_stats"], if_exists="append"
        )

        return profile_stats_data["user"]["secUid"]

    def get_video_stats(self):
        # get secUid
        secuid = self.get_profile_stats()

        # get request for video_stats
        params = {"token": self.tiktok_info["token"], "depth": 2, "secUid": secuid}
        video_stats_data = self.get_request(self.urls["video_stats"], params=params)[
            "data"
        ]

        # setup dataframe
        data = []  # list of rows
        all_user_info = []
        for video in video_stats_data:
            sample = []  # one row
            sample.append(video["aweme_id"])
            sample.append(video["desc"])
            sample.append(video["music"]["title"])
            sample.append(video["statistics"]["comment_count"])
            sample.append(video["statistics"]["digg_count"])
            sample.append(video["statistics"]["download_count"])
            sample.append(video["statistics"]["play_count"])
            sample.append(video["statistics"]["forward_count"])
            sample.append(video["statistics"]["share_count"])

            # get comments
            params = {
                "token": self.tiktok_info["token"],
                "cursor": 0,
                "aweme_id": video["aweme_id"],
            }
            all_comments_data = self.get_request(
                self.urls["video_comments"], params=params
            )

            # handle null response
            if all_comments_data["data"] == None:
                logging.info("---This video doesnt have any comments yet---")
                temp_sample = sample.copy()
                temp_sample.append("")
                temp_sample.append("")

                data.append(temp_sample)
            else:
                for comment in all_comments_data["data"]["comments"]:
                    temp_sample = sample.copy()
                    temp_sample.append(comment["text"])
                    temp_sample.append(comment["user"]["uid"])

                    # single user info
                    user_info = {
                        "id": comment["user"]["uid"],
                        "nickname": comment["user"]["nickname"],
                        "unique_id": comment["user"]["unique_id"],
                        "region": comment["user"]["region"],
                    }

                    if user_info not in all_user_info:
                        all_user_info.append(user_info)

                    data.append(temp_sample)

        columns = [
            "post_id",
            "desc",
            "music",
            "comments",
            "diggs",
            "download",
            "play",
            "forward",
            "share",
            "comment_text",
            "comment_user_id",
        ]
        video_df = pd.DataFrame(columns=columns, data=data)
        self.upload_to_bigquery(
            video_df, self.table_name["video_stats"], if_exists="replace"
        )

        return all_user_info

    def get_user_info(self):
        # get all user info
        all_users = self.get_video_stats()

        user_df = pd.DataFrame(all_users)
        self.upload_to_bigquery(
            user_df, self.table_name["user_info"], if_exists="replace"
        )

    def upload_to_bigquery(self, df, table_name, if_exists):
        logging.info(f"...Uploading {table_name} to BQ...")
        if df.empty:
            logging.warning("No data bro!!!!")
        else:
            gbq.to_gbq(
                df,
                destination_table=f"{self.bigquery_dataset}.{table_name}",
                project_id=self.bigquery_project,
                credentials=self.credentials,
                if_exists=if_exists,
            )
            logging.info(
                f"Successfully uploaded `{self.bigquery_project}.{self.bigquery_dataset}.{table_name}` to BQ"
            )
