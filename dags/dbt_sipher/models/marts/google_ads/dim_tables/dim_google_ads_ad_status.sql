{{- config(
  materialized='table',
) -}}

SELECT
  customer_id
  , campaign_id
  , ad_group_id
  , ad_group_ad_ad_id                             AS ad_id
  , ad_group_ad_ad_app_ad_descriptions            AS ad_descriptions
  , ad_group_ad_ad_app_ad_headlines               AS ad_headlines
  , ad_group_ad_ad_app_ad_html5_media_bundles     AS ad_html5_media_bundles
  , ad_group_ad_ad_app_ad_mandatory_ad_text       AS ad_mandatory_ad_text
  , ad_group_ad_ad_app_ad_youtube_videos          AS ad_youtube_videos
  , ad_group_ad_ad_strength                       AS ad_strength
  , ad_group_ad_ad_system_managed_resource_source AS ad_system_managed_resource_source
  , ad_group_ad_ad_type                           AS ad_type
  , ad_group_ad_policy_summary_approval_status    AS ad_policy_summary_approval_status
  , ad_group_ad_status                            AS ad_status
FROM
  {{ source('raw_google_adwords', 'ads_Ad') }}     
WHERE
  _DATA_DATE = _LATEST_DATE
