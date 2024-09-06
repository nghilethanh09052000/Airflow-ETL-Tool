{{- config(
  materialized='table',
  partition_by={
    "field": "data_date",
    "data_type": "date",
    "granularity": "day"
    }
) -}}

WITH fct_video AS (
  SELECT
  vb.customer_id
  , vb.campaign_id
  , c.campaign_name
  , vb.ad_group_id
  , a.ad_group_name
  , vb.ad_group_ad_ad_id    AS ad_id
  , vb.video_channel_id
  , vb.video_id
  , av.ad_group_ad_status
  , av.video_duration_seconds
  , av.video_title
  , vb.segments_ad_network_type
  , vb.segments_device
  , MAX(      vb.metrics_clicks                  )               AS clicks
  , MAX(      vb.metrics_conversions             )               AS conversions
  , MAX(      vb.metrics_conversions_value       )               AS conversions_value
  , ROUND(MAX(vb.metrics_cost_micros              / 1000000), 2) AS cost
  , MAX(      vb.metrics_impressions             )               AS impressions
  , MAX(      vb.metrics_view_through_conversions)               AS view_through_conversions
  ,           vb._DATA_DATE                                      AS data_date
  FROM
    {{ source('raw_google_adwords', 'ads_VideoBasicStats') }} vb
  LEFT JOIN
    {{ ref('dim_google_ads_video_status') }} av
  ON
    vb.video_id = av.video_id
  LEFT JOIN
    {{ ref('dim_google_ads_campaign_status') }} c 
  ON 
    vb.campaign_id = c.campaign_id AND vb.customer_id = c.customer_id
  LEFT JOIN 
    {{ ref('dim_google_ads_ad_group_status') }} a
  ON
    vb.ad_group_id = a.ad_group_id AND vb.customer_id = a.customer_id
  GROUP BY ALL
)
, ads_video AS (
SELECT 
  customer_id
  , campaign_id
  , campaign_name
  , campaign_advertising_channel_type
  , ad_group_id
  , ad_group_name
  , ad_id
  , ad_descriptions
  , ad_headlines
  , ad_html5_media_bundles
  , ad_mandatory_ad_text
  , ad_youtube_videos
  , ad_strength
  , ad_system_managed_resource_source
  , ad_type
  , ad_policy_summary_approval_status
  , ad_status
  , segments_ad_network_type
  , segments_device
  , segments_day_of_week
  , SUM(active_view_cpm                              ) AS active_view_cpm
  , SUM(active_view_impressions                      ) AS active_view_impressions
  , SUM(active_view_measurability                    ) AS active_view_measurability
  , SUM(active_view_measurable_cost_micros           ) AS active_view_measurable_cost_micros
  , SUM(active_view_measurable_impressions           ) AS active_view_measurable_impressions
  , SUM(active_view_viewability                      ) AS active_view_viewability
  , SUM(average_cost                                 ) AS average_cost
  , SUM(average_cpc                                  ) AS average_cpc
  , SUM(average_cpm                                  ) AS average_cpm
  , SUM(conversions_from_interactions_rate           ) AS conversions_from_interactions_rate
  , SUM(current_model_attributed_conversions         ) AS current_model_attributed_conversions
  , SUM(gmail_forwards                               ) AS gmail_forwards
  , SUM(gmail_saves                                  ) AS gmail_saves
  , SUM(gmail_secondary_clicks                       ) AS gmail_secondary_clicks
  , SUM(interaction_rate                             ) AS interaction_rate
  , SUM(interactions                                 ) AS interactions
  , SUM(value_per_conversion                         ) AS value_per_conversion
  , SUM(value_per_current_model_attributed_conversion) AS value_per_current_model_attributed_conversion
  , data_date
FROM 
  {{ ref('int_google_ads_ad') }} 
WHERE ad_type = 'VIDEO_RESPONSIVE_AD'
GROUP BY ALL
)
, final AS (
  SELECT 
    fv.customer_id
    , fv.campaign_id
    , fv.campaign_name
    , avd.campaign_advertising_channel_type
    , fv.ad_group_id
    , fv.ad_group_name
    , fv.ad_id
    , avd.ad_descriptions
    , avd.ad_headlines
    , avd.ad_html5_media_bundles
    , avd.ad_mandatory_ad_text
    , avd.ad_youtube_videos
    , avd.ad_strength
    , avd.ad_system_managed_resource_source
    , avd.ad_type
    , avd.ad_policy_summary_approval_status
    , avd.ad_status
    , fv.video_channel_id
    , fv.video_id
    , fv.video_duration_seconds
    , fv.video_title
    , fv.segments_ad_network_type
    , fv.segments_device
    , avd.segments_day_of_week
    , avd.active_view_cpm
    , avd.active_view_impressions
    , avd.active_view_measurability
    , avd.active_view_measurable_cost_micros
    , avd.active_view_measurable_impressions
    , avd.active_view_viewability
    , avd.average_cost
    , avd.average_cpc
    , avd.average_cpm
    , fv.clicks
    , fv.conversions
    , avd.conversions_from_interactions_rate
    , fv.conversions_value
    , fv.cost
    , avd.current_model_attributed_conversions
    , avd.gmail_forwards
    , avd.gmail_saves
    , avd.gmail_secondary_clicks
    , fv.impressions
    , avd.interaction_rate
    , avd.interactions
    , avd.value_per_conversion
    , avd.value_per_current_model_attributed_conversion
    , fv.view_through_conversions
    , fv.data_date
  FROM 
    fct_video fv
  LEFT JOIN 
    ads_video avd
  ON 
    fv.campaign_id = avd.campaign_id AND fv.campaign_name = avd.campaign_name AND fv.ad_group_id = avd.ad_group_id
    AND fv.ad_group_name = avd.ad_group_name AND fv.ad_id = avd.ad_id AND fv.segments_ad_network_type = avd.segments_ad_network_type
    AND fv.segments_device = avd.segments_device AND fv.data_date = avd.data_date
)

SELECT * FROM final