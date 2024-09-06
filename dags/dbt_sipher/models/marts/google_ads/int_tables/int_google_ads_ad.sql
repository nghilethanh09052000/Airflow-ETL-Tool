{{- config(
  materialized='table',
  partition_by={
    "field": "data_date",
    "data_type": "date",
    "granularity": "day"
    }
) -}}

WITH fct_ads AS (
  SELECT
    ads.customer_id
    , ads.campaign_id
    , c.campaign_name
    , c.campaign_advertising_channel_type
    , ads.ad_group_id
    , a.ad_group_name
    , ads.ad_group_ad_ad_id AS ad_id
    , ad.ad_descriptions
    , ad.ad_headlines
    , ad.ad_html5_media_bundles
    , ad.ad_mandatory_ad_text
    , ad.ad_youtube_videos
    , ad.ad_strength
    , ad.ad_system_managed_resource_source
    , ad.ad_type
    , ad.ad_policy_summary_approval_status
    , ad.ad_status
    , ads.segments_ad_network_type
    , ads.segments_click_type
    , ads.segments_device
    , ads.segments_day_of_week
    , ROUND(MAX(ads.metrics_active_view_cpm                              ) / 1000000, 2) AS active_view_cpm
    , MAX(      ads.metrics_active_view_impressions                      )               AS active_view_impressions
    , MAX(      ads.metrics_active_view_measurability                    )               AS active_view_measurability
    , MAX(      ads.metrics_active_view_measurable_cost_micros            / 1000000)     AS active_view_measurable_cost_micros
    , MAX(      ads.metrics_active_view_measurable_impressions           )               AS active_view_measurable_impressions
    , MAX(      ads.metrics_active_view_viewability                      )               AS active_view_viewability
    , ROUND(MAX(ads.metrics_average_cost                                  / 1000000), 2) AS average_cost
    , ROUND(MAX(ads.metrics_average_cpc                                   / 1000000), 2) AS average_cpc
    , ROUND(MAX(ads.metrics_average_cpm                                   / 1000000), 2) AS average_cpm
    , MAX(      ads.metrics_clicks                                       )               AS clicks
    , MAX(      ads.metrics_conversions                                  )               AS conversions
    , MAX(      ads.metrics_conversions_from_interactions_rate           )               AS conversions_from_interactions_rate
    , MAX(      ads.metrics_conversions_value                            )               AS conversions_value
    , ROUND(MAX(ads.metrics_cost_micros                                   / 1000000), 2) AS cost
    , MAX(      ads.metrics_current_model_attributed_conversions         )               AS current_model_attributed_conversions
    , MAX(      ads.metrics_gmail_forwards                               )               AS gmail_forwards
    , MAX(      ads.metrics_gmail_saves                                  )               AS gmail_saves
    , MAX(      ads.metrics_gmail_secondary_clicks                       )               AS gmail_secondary_clicks
    , MAX(      ads.metrics_impressions                                  )               AS impressions
    , MAX(      ads.metrics_interaction_rate                             )               AS interaction_rate
    , MAX(      ads.metrics_interactions                                 )               AS interactions
    , MAX(      ads.metrics_value_per_conversion                         )               AS value_per_conversion
    , MAX(      ads.metrics_value_per_current_model_attributed_conversion)               AS value_per_current_model_attributed_conversion
    ,           ads._DATA_DATE                                                           AS data_date
  FROM 
    {{ source('raw_google_adwords', 'ads_AdStats') }} ads
  LEFT JOIN 
    {{ ref('dim_google_ads_ad_status') }} ad
  ON 
    ads.ad_group_ad_ad_id = ad.ad_id
  LEFT JOIN
    {{ ref('dim_google_ads_campaign_status') }} c 
  ON 
    ads.campaign_id = c.campaign_id AND ads.customer_id = c.customer_id
  LEFT JOIN 
    {{ ref('dim_google_ads_ad_group_status') }} a
  ON
    ads.ad_group_id = a.ad_group_id AND ads.customer_id = a.customer_id
  GROUP BY ALL
)

SELECT * FROM fct_ads
  

