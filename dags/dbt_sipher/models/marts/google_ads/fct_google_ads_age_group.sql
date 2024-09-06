{{- config(
  materialized='table',
  partition_by={
    "field": "data_date",
    "data_type": "date",
    "granularity": "day"
    }
) -}}

WITH age AS (
  SELECT
    ag.customer_id
    , ag.campaign_id
    , c.campaign_name
    , c.campaign_advertising_channel_type
    , ag.ad_group_id
    , a.ad_group_name
    , ag.ad_group_criterion_criterion_id   AS age_group_id
    , ag.ad_group_criterion_age_range_type AS age_group_name
    , ag.ad_group_criterion_status         AS age_group_status
    , ag.campaign_bidding_strategy_type
    , arb.segments_ad_network_type
    , arb.segments_device
    ,       MAX(arb.metrics_active_view_impressions                         ) AS active_view_impressions
    ,       MAX(arb.metrics_active_view_measurability                       ) AS active_view_measurability
    , ROUND(MAX(arb.metrics_active_view_measurable_cost_micros)/ 1000000, 2 ) AS active_view_measurable_cost_micros
    ,       MAX(arb.metrics_active_view_measurable_impressions              ) AS active_view_measurable_impressions
    ,       MAX(arb.metrics_active_view_viewability                         ) AS active_view_viewability
    ,       MAX(arb.metrics_all_conversions                                 ) AS all_conversions
    ,       MAX(arb.metrics_all_conversions_value                           ) AS all_conversions_value
    ,       MAX(arb.metrics_clicks                                          ) AS clicks
    ,       MAX(arb.metrics_conversions                                     ) AS conversions
    ,       MAX(arb.metrics_conversions_value                               ) AS conversions_value
    , ROUND(MAX(arb.metrics_cost_micros)                        / 1000000, 2) AS cost
    ,       MAX(arb.metrics_cross_device_conversions                        ) AS cross_device_conversions
    ,       MAX(arb.metrics_impressions                                     ) AS impressions
    ,       MAX(arb.metrics_interactions                                    ) AS interactions
    ,       MAX(arb.metrics_view_through_conversions                        ) AS view_through_conversions
    , arb._DATA_DATE AS data_date
  FROM
    {{ source('raw_google_adwords', 'ads_AgeRange') }} ag
  LEFT JOIN
    {{ ref('dim_google_ads_campaign_status') }} c 
  ON 
    ag.campaign_id = c.campaign_id AND ag.customer_id = c.customer_id
  LEFT JOIN 
    {{ ref('dim_google_ads_ad_group_status') }} a
  ON
    ag.ad_group_id = a.ad_group_id AND ag.customer_id = a.customer_id
  LEFT JOIN
    {{ source('raw_google_adwords', 'ads_AgeRangeBasicStats') }} arb
  ON
    ag.customer_id = arb.customer_id AND ag.campaign_id = arb.campaign_id AND ag.ad_group_id = arb.ad_group_id AND ag.ad_group_criterion_criterion_id = arb.ad_group_criterion_criterion_id
  WHERE
    ag._DATA_DATE = ag._LATEST_DATE
  GROUP BY ALL

)

SELECT * FROM age