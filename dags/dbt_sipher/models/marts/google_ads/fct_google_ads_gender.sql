{{- config(
  materialized='table',
  partition_by={
    "field": "data_date",
    "data_type": "date",
    "granularity": "day"
  }
) -}}

WITH gen AS (
 SELECT
  ge.customer_id
  , ge.campaign_id
  , c.campaign_name
  , c.campaign_advertising_channel_type
  , ge.ad_group_id
  , a.ad_group_name
  , ge.ad_group_criterion_criterion_id AS ad_gender_id
  , ge.ad_group_criterion_gender_type  AS ad_gender_type
  , ge.ad_group_criterion_status       AS ad_gender_status
  , ge.bidding_strategy_type
  , gb.segments_ad_network_type
  , gb.segments_device
  ,       MAX(gb.metrics_active_view_impressions                         ) AS active_view_impressions
  ,       MAX(gb.metrics_active_view_measurability                       ) AS active_view_measurability
  , ROUND(MAX(gb.metrics_active_view_measurable_cost_micros)/ 1000000, 2 ) AS active_view_measurable_cost_micros
  ,       MAX(gb.metrics_active_view_measurable_impressions              ) AS active_view_measurable_impressions
  ,       MAX(gb.metrics_active_view_viewability                         ) AS active_view_viewability
  ,       MAX(gb.metrics_all_conversions                                 ) AS all_conversions
  ,       MAX(gb.metrics_all_conversions_value                           ) AS all_conversions_value
  ,       MAX(gb.metrics_clicks                                          ) AS clicks
  ,       MAX(gb.metrics_conversions                                     ) AS conversions
  ,       MAX(gb.metrics_conversions_value                               ) AS conversions_value
  , ROUND(MAX(gb.metrics_cost_micros)                        / 1000000, 2) AS cost
  ,       MAX(gb.metrics_cross_device_conversions                        ) AS cross_device_conversions
  ,       MAX(gb.metrics_impressions                                     ) AS impressions
  ,       MAX(gb.metrics_interactions                                    ) AS interactions
  ,       MAX(gb.metrics_view_through_conversions                        ) AS view_through_conversions
  , gb._DATA_DATE AS data_date
 FROM
  {{ source('raw_google_adwords', 'ads_Gender') }} ge
 LEFT JOIN
  {{ ref('dim_google_ads_campaign_status') }} c 
 ON 
  ge.campaign_id = c.campaign_id AND ge.customer_id = c.customer_id
 LEFT JOIN 
  {{ ref('dim_google_ads_ad_group_status') }} a
 ON
  ge.ad_group_id = a.ad_group_id AND ge.customer_id = a.customer_id
 LEFT JOIN
  {{ source('raw_google_adwords', 'ads_GenderBasicStats') }} gb
 ON
  ge.customer_id = gb.customer_id AND ge.campaign_id = gb.campaign_id AND ge.ad_group_id = gb.ad_group_id AND ge.ad_group_criterion_criterion_id = gb.ad_group_criterion_criterion_id
 WHERE
  ge._DATA_DATE = ge._LATEST_DATE
 GROUP BY ALL

)

SELECT * FROM gen