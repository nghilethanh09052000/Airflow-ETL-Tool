{{- config(
    materialized = 'table',
    partition_by={
      'field': 'date_tzutc',
      'data_type': 'DATE',
  }
)-}}

WITH co AS (
  SELECT 
      date_tzutc
      , app_name 
      , media_source
      , platform
      , SUM(cost_usd) AS cost_usd
      , source
  FROM {{ ref('fct_fin_incubator_cost')}}
  GROUP BY 1,2,3,4,6
), re AS (
    SELECT 
      date_tzutc
      , app_name
      , media_source
      , ad_network
      , event_name
      , monetization_network
      , platform 
      , CASE 
          WHEN event_name = "af_purchase"  AND source = 'appsflyer'     THEN SUM(est_revenue_usd)
          ELSE 0
          END AS revenue_iap_usd
      , CASE
          WHEN event_name != "af_purchase" AND source = 'max_mediation' THEN SUM(est_revenue_usd)
          ELSE 0
      END AS revenue_ads_usd
      , source
    FROM {{ ref('fct_fin_incubator_revenue')}}
    GROUP BY 1,2,3,4,5,6,7,10
), rev AS (
    SELECT 
      date_tzutc
      , app_name
      , media_source
      , ad_network
      , platform
      , SUM(revenue_iap_usd) AS revenue_iap_usd
      , SUM(revenue_ads_usd) AS revenue_ads_usd
      , source 
    FROM re 
    GROUP BY 1,2,3,4,5,8
), jo AS (
    SELECT 
      date_tzutc
      , app_name
      , media_source
      , COALESCE(ad_network, "") AS ad_network
      , platform
      , COALESCE(cost_usd, 0) AS cost_usd
      , COALESCE((revenue_ads_usd + revenue_iap_usd), 0) AS revenue_usd
      , COALESCE(revenue_iap_usd, 0) AS revenue_iap_usd
      , COALESCE(revenue_ads_usd, 0) AS revenue_ads_usd
    FROM rev
    FULL JOIN co 
    USING (date_tzutc,  app_name, media_source, platform, source)
), final AS (
  SELECT 
    date_tzutc
    , app_name
    , media_source
    , ad_network
    , platform
    , cost_usd
    , revenue_usd
    , revenue_iap_usd
    , revenue_ads_usd
    , (revenue_usd - cost_usd) AS profit
  FROM jo

)

SELECT * FROM final
