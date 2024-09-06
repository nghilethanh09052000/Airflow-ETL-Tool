from enum import Enum
from typing import List, Union
import pandas as pd
from social.hooks.facebook.base import FacebookBaseHook
import logging
import json
from pandas import json_normalize

class CommentFilter(Enum):
    TOPLEVEL = "toplevel"
    STREAM = "stream"


class FacebookPagePostHook(FacebookBaseHook):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.access_token = self.get_connection(self.http_conn_id).extra_dejson[
            "access_token"
        ]
        self.version = self.get_connection(self.http_conn_id).extra_dejson["version"]

    @FacebookBaseHook.paginator
    def get_feed(
        self,
        page_id: str,
        fields: Union[str, List[str]],
        after: str = None,
        limit: int = 100,
    ):
        """
        ..seealso:
            https://developers.facebook.com/docs/graph-api/reference/v18.0/page/feed
        """
        endpoint = f"/{self.version}/{page_id}/feed"
        if isinstance(fields, str):
            parsed_fields = fields
        elif isinstance(fields, list):
            parsed_fields = ",".join(fields)

        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "access_token": self.access_token,
                "fields": parsed_fields,
                "after": after,
                "limit": limit,
            },
        )
        return response
    
    @FacebookBaseHook.paginator
    def get_overall(
        self,
        page_id: str,
        fields: Union[str, List[str]],
        after: str = None,
        limit: int = 100,
    ):
        """
        ..seealso:
            https://developers.facebook.com/docs/graph-api/reference/v18.0/page
        """
        endpoint = f"/{self.version}/{page_id}/"
        if isinstance(fields, str):
            parsed_fields = fields
        elif isinstance(fields, list):
            parsed_fields = ",".join(fields)

        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "access_token": self.access_token,
                "fields": parsed_fields,
                "after": after,
                "limit": limit,
            },
        )
        return response
    
    def get_page_insight(
        self, page_id: str, metrics: List[str], ds: str,period: str = "day", 
    ):
        """https://developers.facebook.com/docs/graph-api/reference/v18.0/insights"""

        endpoint = f"/{self.version}/{page_id}/insights"
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "access_token": self.access_token,
                "metric": ",".join(metrics),
                "period": period,
                "since": ds,
                "until": ds,
                "limit": 100,
            },
        )
        self.check_response(response)
        data = response.json()
        for item in data["data"]:
            if isinstance(item["values"][0]["value"], dict):
                if item["values"][0]["value"] == {} :
                    item["values"][0]["value"] = ""
                else: 
                    item["values"][0]["value"] = json.dumps(item["values"][0]["value"])
        flat_data = json_normalize(data['data'])
        flattened_values = json_normalize(flat_data['values'].apply(lambda x: x[0]))
        result_df = pd.concat([flat_data, flattened_values], axis=1).drop('values', axis=1)

        result_df = result_df.pivot_table(index=['period', 'end_time'],
                                        columns='name', values='value', aggfunc='first').reset_index()
        
        return result_df.to_dict(orient='records')

    def get_post_insight(
        self, post_ids: List[str], metrics: List[str], period: str = "lifetime"
    ):
        """https://developers.facebook.com/docs/graph-api/reference/v18.0/insights"""

        post_ids = self._parse_xcom_list(post_ids)
        result = pd.DataFrame()
        for post_id in post_ids:
            self.log.info("Getting insights for post `%s`", post_id)
            endpoint = f"/{self.version}/{post_id}/insights"
            response = self.make_http_request(
                endpoint=endpoint,
                params={
                    "access_token": self.access_token,
                    "metric": ",".join(metrics),
                    "period": period,
                    "limit": 100,
                },
            )
            if response is not None:
                data = response.json()
                flat_data = json_normalize(data['data'])
                flattened_values = json_normalize(flat_data['values'].apply(lambda x: x[0]))
                result_df = pd.concat([flat_data, flattened_values], axis=1).drop('values', axis=1)

                result_df = result_df.pivot_table(index=['period'],
                                                columns='name', values='value', aggfunc='first').reset_index()
                result_df["post_id"] = post_id
                result = pd.concat([result, result_df], ignore_index=True)

        return result.to_dict(orient='records')

    def get_post_comments(
        self,
        post_ids: List[str],
        summary: bool = True,
        filter: CommentFilter = CommentFilter.STREAM.value,
    ):
        """https://developers.facebook.com/docs/graph-api/reference/v18.0/object/comments"""
        data = []
        post_ids = self._parse_xcom_list(post_ids)
        for post_id in post_ids:
            comment = self._get_comments_all_posts(
                post_id=post_id, summary=summary, filter=filter
            )
            logging.info(comment)
            if len(comment) == 0:
                logging.info(f"No comment in {post_id}")
            data.append({"comment": comment, "post_id": post_id})
        result = pd.json_normalize([
            {**{'post_id': item['post_id']}, **comment} 
            for item in data
            for comment in item['comment']
        ])
        return result.to_dict(orient='records')

    def _get_comments_all_posts(
        self,
        post_id: str,
        summary: bool = True,
        filter: CommentFilter = CommentFilter.STREAM.value,
    ):
        """https://developers.facebook.com/docs/graph-api/reference/v18.0/object/comments"""

        response, get_next= self._get_single_post_comments(
            post_id=post_id, summary=summary, filter=filter
        )
        data = []
        while response and response.status_code == 200:
            data.extend(response.json()["data"])
            if get_next:
                response, get_next = get_next()
            else:
                break      
        return data
    
    @FacebookBaseHook.paginator
    def _get_single_post_comments(
        self,
        post_id: str,
        summary: bool = True,
        filter: CommentFilter = CommentFilter.STREAM.value,

        after: str = None,
        limit: int = 100,
    ):
        """https://developers.facebook.com/docs/graph-api/reference/v18.0/object/comments"""
        endpoint = f"/{self.version}/{post_id}/comments"
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "access_token": self.access_token,
                "summary": summary,
                "filter": filter,
                "after": after,
                "limit": limit,
            },
        )
        return response

    def get_page_impression_gender_locate(
        self, page_id: str, metrics: List[str], ds: str,period: str = "day", 
    ):
        """https://developers.facebook.com/docs/graph-api/reference/v18.0/insights"""

        endpoint = f"/{self.version}/{page_id}/insights"
        response = self.make_http_request(
            endpoint=endpoint,
            params={
                "access_token": self.access_token,
                "metric": ",".join(metrics),
                "period": period,
                "since": ds,
                "until": ds,
                "limit": 100,
            },
        )
        self.check_response(response)
        data = response.json()["data"]

        #replace "." in field name to upload to bigquery
        for item in data:
            for value in item.get("values", []):
                value["value"] = {k.replace(".", "_"): v for k, v in value.get("value", {}).items()}
        
        flattened_data = [
            {
                "name": item["name"],
                "period": item["period"],
                "title": item["title"],
                "description": item["description"],
                "end_time": item["values"][0]["end_time"],
                "metric": key,
                "reach": value
            }

            for item in data
            for key, value in item["values"][0]["value"].items()
        ]
        print(flattened_data)
        return flattened_data

    @staticmethod
    def _parse_xcom_list(xcom_list: str):
        """Airflow template return xcom_list as string instead of list"""
        if isinstance(xcom_list, str):
            return eval(xcom_list)
        elif isinstance(xcom_list, List):
            return xcom_list
