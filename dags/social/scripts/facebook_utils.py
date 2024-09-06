import logging
from enum import Enum

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import bigquery

BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")
class Pages(Enum):
    Sipher = "105660785134129"


class Params_fb:
    PAGE_INSIGHTS_METRICS = [
## Page Content
        "page_tab_views_login_top_unique",
        "page_tab_views_login_top",
        "page_tab_views_logout_top",
        # "page_engaged_users", --were deprecated - 2024.03
## Page Engagement
        # "page_engaged_users", 
        "page_post_engagements", 
        # "page_consumptions", --were deprecated - 2024.03
        "page_consumptions_unique", 
        "page_consumptions_by_consumption_type", 
        "page_consumptions_by_consumption_type_unique", 
        "page_places_checkin_total", 
        "page_places_checkin_total_unique", 
        # "page_places_checkin_mobile",          --were deprecated - 2024.03
        # "page_places_checkin_mobile_unique",   --were deprecated - 2024.03
        # "page_places_checkins_by_age_gender",  --were deprecated - 2024.03
        # "page_places_checkins_by_locale",      --were deprecated - 2024.03
        # "page_places_checkins_by_country",     --were deprecated - 2024.03
        "page_negative_feedback", 
        "page_negative_feedback_unique", 
        "page_negative_feedback_by_type", 
        "page_negative_feedback_by_type_unique", 
        # "page_positive_feedback_by_type",             --were deprecated - 2024.03
        # "page_positive_feedback_by_type_unique",      --were deprecated - 2024.03
        "page_fans_online", 
        "page_fans_online_per_day", 
        # "page_fan_adds_by_paid_non_paid_unique",
## Page Impressions
        "page_impressions", 
        "page_impressions_unique", 
        "page_impressions_paid", 
        "page_impressions_paid_unique", 
        "page_impressions_organic_v2", 
        "page_impressions_organic_unique_v2", 
        "page_impressions_viral", 
        "page_impressions_viral_unique", 
        "page_impressions_nonviral", 
        "page_impressions_nonviral_unique", 
        "page_impressions_by_story_type", 
        "page_impressions_by_story_type_unique", 
        "page_impressions_by_city_unique", 
        "page_impressions_by_country_unique", 
        "page_impressions_by_locale_unique", 
        "page_impressions_by_age_gender_unique", 
        # "page_impressions_frequency_distribution",  --were deprecated - 2024.03
        "page_impressions_viral_frequency_distribution",
## Page Posts

        "page_posts_impressions", 
        "page_posts_impressions_unique", 
        "page_posts_impressions_paid", 
        "page_posts_impressions_paid_unique", 
        "page_posts_impressions_organic", 
        "page_posts_impressions_organic_unique", 
        "page_posts_served_impressions_organic_unique", 
        "page_posts_impressions_viral", 
        "page_posts_impressions_viral_unique", 
        "page_posts_impressions_nonviral", 
        "page_posts_impressions_nonviral_unique", 
        # "page_posts_impressions_frequency_distribution", --were deprecated - 2024.03
## Page Reactions
        "page_actions_post_reactions_like_total", 
        "page_actions_post_reactions_love_total", 
        "page_actions_post_reactions_wow_total", 
        "page_actions_post_reactions_haha_total", 
        "page_actions_post_reactions_sorry_total", 
        "page_actions_post_reactions_anger_total", 
        # "page_actions_post_reactions_total",
## Page User Demographics
        "page_fans", 
        "page_fans_locale", 
        "page_fans_city", 
        "page_fans_country", 
        # "page_fans_gender_age",             --were deprecated - 2024.03
        "page_fan_adds", 
        "page_fan_adds_unique", 
        # "page_fans_by_like_source",         --were deprecated - 2024.03
        # "page_fans_by_like_source_unique",  --were deprecated - 2024.03
        "page_fan_removes", 
        "page_fan_removes_unique", 
        # "page_fans_by_unlike_source_unique",
## Page Video Views
        "page_video_views", 
        "page_video_views_by_uploaded_hosted", 
        "page_video_views_paid", 
        "page_video_views_organic", 
        # "page_video_views_by_paid_non_paid", 
        "page_video_views_autoplayed", 
        "page_video_views_click_to_play", 
        "page_video_views_unique", 
        "page_video_repeat_views", 
        "page_video_complete_views_30s", 
        "page_video_complete_views_30s_paid", 
        "page_video_complete_views_30s_organic", 
        "page_video_complete_views_30s_autoplayed", 
        "page_video_complete_views_30s_click_to_play", 
        "page_video_complete_views_30s_unique", 
        "page_video_complete_views_30s_repeat_views", 
        "post_video_complete_views_30s_autoplayed", 
        "post_video_complete_views_30s_clicked_to_play", 
        "post_video_complete_views_30s_organic", 
        "post_video_complete_views_30s_paid", 
        "post_video_complete_views_30s_unique", 
#--Deprecated above Graph API v18---
        # "page_video_views_10s", 
        # "page_video_views_10s_paid", 
        # "page_video_views_10s_organic", 
        # "page_video_views_10s_autoplayed", 
        # "page_video_views_10s_click_to_play", 
        # "page_video_views_10s_unique", 
        # "page_video_views_10s_repeat", 
#--Deprecated above Graph API v18---
        "page_video_view_time",
## Page Views
        "page_views_total", 
        # "page_views_logout",                                --were deprecated - 2024.03
        # "page_views_logged_in_total",                       --were deprecated - 2024.03
        # "page_views_logged_in_unique",                      --were deprecated - 2024.03
        # "page_views_external_referrals",                    --were deprecated - 2024.03
        # "page_views_by_profile_tab_total",                  --were deprecated - 2024.03
        # "page_views_by_profile_tab_logged_in_unique",       --were deprecated - 2024.03
        # "page_views_by_internal_referer_logged_in_unique",  --were deprecated - 2024.03
        # "page_views_by_site_logged_in_unique",              --were deprecated - 2024.03
        # "page_views_by_age_gender_logged_in_unique",        --were deprecated - 2024.03
        # "page_views_by_referers_logged_in_unique",          --were deprecated - 2024.03
## Stories
        # "page_content_activity_by_action_type_unique", --were deprecated - 2024.03
        # "page_content_activity_by_age_gender_unique",  --were deprecated - 2024.03
        # "page_content_activity_by_city_unique",        --were deprecated - 2024.03
        # "page_content_activity_by_country_unique",     --were deprecated - 2024.03
        # "page_content_activity_by_locale_unique",      --were deprecated - 2024.03
        # "page_content_activity",                       --were deprecated - 2024.03
        # "page_content_activity_by_action_type",        --were deprecated - 2024.03
## Video Ad Breaks
        "page_daily_video_ad_break_ad_impressions_by_crosspost_status", 
        "page_daily_video_ad_break_cpm_by_crosspost_status", 
        "page_daily_video_ad_break_earnings_by_crosspost_status", 
    ]

    POST_METRICS = [
## Post Engagement     
        "post_engaged_users", 
        "post_negative_feedback", 
        "post_negative_feedback_unique", 
        "post_negative_feedback_by_type", 
        "post_negative_feedback_by_type_unique", 
        "post_engaged_fan", 
        "post_clicks", 
        "post_clicks_unique", 
        "post_clicks_by_type", 
        "post_clicks_by_type_unique",
## Post Impressions
        "post_impressions", 
        "post_impressions_unique", 
        "post_impressions_paid", 
        "post_impressions_paid_unique", 
        "post_impressions_fan", 
        "post_impressions_fan_unique", 
        # "post_impressions_fan_paid",         --were deprecated - 2024.03
        # "post_impressions_fan_paid_unique",  --were deprecated - 2024.03
        "post_impressions_organic", 
        "post_impressions_organic_unique",
        "post_impressions_viral", 
        "post_impressions_viral_unique", 
        "post_impressions_nonviral", 
        "post_impressions_nonviral_unique", 
        "post_impressions_by_story_type", 
        "post_impressions_by_story_type_unique",
## Post Reactions
        "post_reactions_like_total", 
        "post_reactions_love_total", 
        "post_reactions_wow_total", 
        "post_reactions_haha_total", 
        "post_reactions_sorry_total", 
        "post_reactions_anger_total", 
        "post_reactions_by_type_total",
    ]

    POST_FIELDS = [
        "id",
        "message",
        "attachments",
        "created_time",
        "from",
        "full_picture",
        "instagram_eligibility",
        "is_eligible_for_promotion",
        "is_expired",
        "is_hidden",
        "is_instagram_eligible",
        "is_popular",
        "is_published",
        "is_spherical",
        "message_tags",
        "parent_id",
        "permalink_url",
        "privacy",
        "promotable_id",
        "properties",
        "shares",
        "status_type",
        "story_tags",
        "subscribed",
        "to",
        "updated_time",
        "video_buying_eligibility",
    ]

    PAGE_FIELDS = [
        "id",
        "name",
        "username",
        "category",
        "followers_count",
        "is_community_page",
        "link",
    ]

    PAGE_IMPRESSION_GENDER_LOCATE = [
        "page_impressions_by_age_gender_unique",
        "page_impressions_by_country_unique",
    ]

    REPORT_TYPES = {
                "page_overall" : {"partition_expr": "{page_id:STRING}/{snapshot_date:DATE}"},
                "page_insights": {"partition_expr": "{page_id:STRING}/{snapshot_date:DATE}"},
                "page_feed"    : {"partition_expr": "{page_id:STRING}/{snapshot_date:DATE}"},
                "post_insights": {"partition_expr": "{snapshot_date:DATE}"},  
                "post_comments": {"partition_expr": "{snapshot_date:DATE}"},
                "page_impression_gender_locate" : {"partition_expr": "{page_id:STRING}/{snapshot_date:DATE}"},
    }


def get_facebook_post_ids(gcp_conn_id: str, bq_project: str, **kwargs):
    client = bigquery.Client.from_service_account_json(
        BaseHook.get_connection(gcp_conn_id).extra_dejson[
            "key_path"
        ]
    )
    ds = kwargs.get("ds")
    query = f"""
        SELECT DISTINCT post_id
        FROM `{bq_project}.staging_social.stg_facebook_page_feed`
        WHERE DATE(created_time) BETWEEN DATE_SUB("{ds}", INTERVAL 30 DAY) AND "{ds}"

    """
    logging.info("Run query check...")
    print(query)

    rows = client.query(query, project=BIGQUERY_BILLING_PROJECT).result()
    res = [row.post_id for row in rows]
    logging.info("Found %s posts", len(res))
    return res
