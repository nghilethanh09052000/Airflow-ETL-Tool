{{- config(
    materialized='view',
)-}}

WITH final AS (
SELECT
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%z", end_time) AS end_time
  , page_id
  , period
  , page_actions_post_reactions_like_total
  , page_actions_post_reactions_love_total
  , page_actions_post_reactions_wow_total
  , page_actions_post_reactions_haha_total 
  , page_actions_post_reactions_sorry_total 
  , page_actions_post_reactions_anger_total
--  , page_consumptions
  , page_consumptions_unique
  , CAST(JSON_EXTRACT_SCALAR(page_consumptions_by_consumption_type, '$.other clicks')        AS INT64) AS page_consumptions_by_other_clicks
  , CAST(JSON_EXTRACT_SCALAR(page_consumptions_by_consumption_type_unique, '$.other clicks') AS INT64) AS page_consumptions_by_other_clicks_unique
  , CAST(JSON_EXTRACT_SCALAR(page_consumptions_by_consumption_type, '$.photo view')         AS INT64) AS page_consumptions_by_photo_view
  , CAST(JSON_EXTRACT_SCALAR(page_consumptions_by_consumption_type_unique, '$.photo view')  AS INT64) AS page_consumptions_by_photo_view_unique
--  , page_content_activity
--  , CAST(JSON_EXTRACT_SCALAR(page_content_activity_by_action_type, '$.fan')              AS INT64) AS page_content_activity_by_fan
--  , CAST(JSON_EXTRACT_SCALAR(page_content_activity_by_action_type_unique, '$.fan')       AS INT64) AS page_content_activity_by_fan_unique
--  , CAST(JSON_EXTRACT_SCALAR(page_content_activity_by_action_type, '$.page post')        AS INT64) AS page_content_activity_by_page_post
--  , CAST(JSON_EXTRACT_SCALAR(page_content_activity_by_action_type_unique, '$.page post') AS INT64) AS page_content_activity_by_page_post_unique
--  , CAST(JSON_EXTRACT_SCALAR(page_content_activity_by_action_type, '$.other')            AS INT64) AS page_content_activity_by_other
--  , CAST(JSON_EXTRACT_SCALAR(page_content_activity_by_action_type_unique, '$.other')     AS INT64) AS page_content_activity_by_other_unique
--  , page_engaged_users
  , page_fans
  , page_fan_removes
  , page_fan_removes_unique
--  , page_fans_by_like_source
--  , page_fans_by_like_source_unique
--  , page_fans_by_unlike_source_unique
  , page_fans_online
  , page_impressions
  , CAST(JSON_EXTRACT_SCALAR(page_impressions_by_story_type, '$.page post')        AS INT64) AS page_impressions_by_page_post
  , CAST(JSON_EXTRACT_SCALAR(page_impressions_by_story_type_unique, '$.page post') AS INT64) AS page_impressions_by_page_post_unique
  , CAST(JSON_EXTRACT_SCALAR(page_impressions_by_story_type, '$.checkin')          AS INT64) AS page_impressions_by_checkin
  , CAST(JSON_EXTRACT_SCALAR(page_impressions_by_story_type_unique, '$.checkin')   AS INT64) AS page_impressions_by_checkin_unique
--  , page_impressions_frequency_distribution
  , page_impressions_nonviral
  , page_impressions_nonviral_unique
  , page_impressions_organic_unique_v2
  , page_impressions_organic_v2
  , page_impressions_paid
  , page_impressions_paid_unique
  , page_impressions_unique
  , page_impressions_viral
  , page_impressions_viral_frequency_distribution
  , page_impressions_viral_unique
  , page_negative_feedback
  , page_negative_feedback_by_type
  , page_negative_feedback_by_type_unique
  , page_negative_feedback_unique
  , page_places_checkin_total
  , page_places_checkin_total_unique
--  , page_places_checkin_mobile
--  , page_places_checkin_mobile_unique
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type, '$.link')            AS INT64) AS page_positive_feedback_by_link
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type_unique, '$.link')     AS INT64) AS page_positive_feedback_by_link_unique
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type, '$.like')            AS INT64) AS page_positive_feedback_by_like
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type_unique, '$.like')     AS INT64) AS page_positive_feedback_by_like_unique
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type, '$.comment')         AS INT64) AS page_positive_feedback_by_comment
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type_unique, '$.comment')  AS INT64) AS page_positive_feedback_by_comment_unique
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type, '$.other')           AS INT64) AS page_positive_feedback_by_other
--  , CAST(JSON_EXTRACT_SCALAR(page_positive_feedback_by_type_unique, '$.other')    AS INT64) AS page_positive_feedback_by_other_unique
  , page_post_engagements
  , page_posts_impressions
--  , page_posts_impressions_frequency_distribution
  , page_posts_impressions_nonviral
  , page_posts_impressions_nonviral_unique
  , page_posts_impressions_organic
  , page_posts_impressions_organic_unique
  , page_posts_impressions_paid
  , page_posts_impressions_paid_unique
  , page_posts_impressions_unique
  , page_posts_impressions_viral
  , page_posts_impressions_viral_unique
  , page_posts_served_impressions_organic_unique
  , page_tab_views_login_top
  , page_tab_views_login_top_unique
  , page_tab_views_logout_top
  , page_video_complete_views_30s
  , page_video_complete_views_30s_autoplayed
  , page_video_complete_views_30s_click_to_play
  , page_video_complete_views_30s_organic
  , page_video_complete_views_30s_paid
  , page_video_complete_views_30s_repeat_views
  , page_video_complete_views_30s_unique
  , page_video_repeat_views
  , page_video_view_time
  , page_video_views
  , page_video_views_autoplayed
  , CAST(JSON_EXTRACT_SCALAR(page_video_views_by_uploaded_hosted, '$.page_uploaded')                 AS INT64) AS page_video_views_by_page_uploaded
  , CAST(JSON_EXTRACT_SCALAR(page_video_views_by_uploaded_hosted, '$.page_uploaded_from_crossposts') AS INT64) AS page_video_views_by_page_uploaded_from_crossposts
  , CAST(JSON_EXTRACT_SCALAR(page_video_views_by_uploaded_hosted, '$.page_uploaded_from_shares')     AS INT64) AS page_video_views_by_page_uploaded_from_shares
  , CAST(JSON_EXTRACT_SCALAR(page_video_views_by_uploaded_hosted, '$.page_hosted_crosspost')         AS INT64) AS page_video_views_by_page_hosted_crosspost
  , CAST(JSON_EXTRACT_SCALAR(page_video_views_by_uploaded_hosted, '$.page_hosted_share')             AS INT64) AS page_video_views_by_page_hosted_share
  , CAST(JSON_EXTRACT_SCALAR(page_video_views_by_uploaded_hosted, '$.page_owned')                    AS INT64) AS page_video_views_by_page_owned
  , page_video_views_click_to_play
  , page_video_views_organic
  , page_video_views_paid
  , page_video_views_unique
--  , page_views_external_referrals
--  , page_views_logged_in_total
--  , page_views_logged_in_unique
  , page_views_total
--  , page_views_by_referers_logged_in_unique
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_internal_referer_logged_in_unique, '$.OTHER')  AS INT64) AS page_views_by_internal_referer_logged_in_unique_other
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_internal_referer_logged_in_unique, '$.NONE')   AS INT64) AS page_views_by_internal_referer_logged_in_unique_none
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_internal_referer_logged_in_unique, '$.SEARCH') AS INT64) AS page_views_by_internal_referer_logged_in_unique_search
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_profile_tab_logged_in_unique, '$.PHOTOS')      AS INT64) AS page_views_by_profile_tab_logged_in_unique_photos
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_profile_tab_logged_in_unique, '$.HOME')        AS INT64) AS page_views_by_profile_tab_logged_in_unique_home
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_profile_tab_logged_in_unique, '$.ABOUT')       AS INT64) AS page_views_by_profile_tab_logged_in_unique_about
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_profile_tab_total, '$.PHOTOS')                 AS INT64) AS page_views_by_profile_tab_total_photos
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_profile_tab_total, '$.HOME')                   AS INT64) AS page_views_by_profile_tab_total_home
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_profile_tab_total, '$.ABOUT')                  AS INT64) AS page_views_by_profile_tab_total_about
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_site_logged_in_unique, '$.OTHER')              AS INT64) AS page_views_by_site_logged_in_unique_other
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_site_logged_in_unique, '$.MOBILE')             AS INT64) AS page_views_by_site_logged_in_unique_mobile
--  , CAST(JSON_EXTRACT_SCALAR(page_views_by_site_logged_in_unique, '$.WWW')                AS INT64) AS page_views_by_site_logged_in_unique_www

  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'facebook_sipher_page_insights') }}

)

SELECT * FROM final




