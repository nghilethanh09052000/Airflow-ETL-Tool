{{- config(
  materialized='table',
  partition_by={
    "field": "data_date",
    "data_type": "date",
    "granularity": "day"
    }
) -}}

WITH video AS (
    SELECT
        * 
    FROM
        {{ref('int_google_ads_video')}}
)
, ads AS (
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
        , "" AS video_channel_id
        , "" AS video_id
        , 0 AS video_duration_seconds
        , "" AS video_title
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
        , SUM(clicks                                       ) AS clicks
        , SUM(conversions                                  ) AS conversions
        , SUM(conversions_from_interactions_rate           ) AS conversions_from_interactions_rate
        , SUM(conversions_value                            ) AS conversions_value
        , SUM(cost                                         ) AS cost
        , SUM(current_model_attributed_conversions         ) AS current_model_attributed_conversions
        , SUM(gmail_forwards                               ) AS gmail_forwards
        , SUM(gmail_saves                                  ) AS gmail_saves
        , SUM(gmail_secondary_clicks                       ) AS gmail_secondary_clicks
        , SUM(impressions                                  ) AS impressions
        , SUM(interaction_rate                             ) AS interaction_rate
        , SUM(interactions                                 ) AS interactions
        , SUM(value_per_conversion                         ) AS value_per_conversion
        , SUM(value_per_current_model_attributed_conversion) AS value_per_current_model_attributed_conversion
        , 0 AS view_through_conversions
        , data_date
    FROM 
    {{ ref('int_google_ads_ad') }} 
    WHERE ad_type != 'VIDEO_RESPONSIVE_AD'
    GROUP BY 
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24, data_date
)

SELECT * FROM video
UNION ALL
SELECT * FROM ads

