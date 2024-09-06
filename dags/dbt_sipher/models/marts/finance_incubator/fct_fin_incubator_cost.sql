{{- config(
    materialized = 'table',
    partition_by={
		'field': 'date_tzutc',
		'data_type': 'DATE',
  }
)-}}

WITH installs AS (
  SELECT
    date_tzutc
    , CASE 
      WHEN app_id = 'id6462842931'                               THEN 'Hidden Atlas'
      WHEN app_id = 'hidden.atlas.seek.spot.objects.zoom.maps2d' THEN 'Hidden Atlas: Anime Zen Object'
      WHEN app_id = 'com.atherlabs.sipherodyssey'                THEN 'Sipher Odyssey'
      WHEN app_id = 'id6471829700'                               THEN 'Watermelon 2048: Merge Fruits'
      ELSE app_id
    END AS app_name
    , media_source
    , '' AS ad_network
    , '' AS monetization_network
    , '' AS ad_format
    , '' AS event_name 
    , campaign
    , af_adset
    , af_ad
    , platform
    , is_organic
    , country_code
    , af_cost_model
    , af_cost_value
    , af_cost_currency
    , 'appsflyer' AS source
    , 0           AS est_revenue_usd
    , 0           AS impressions
    , 0           AS est_epcm
  FROM {{ ref('stg_appsflyer__installs')}}
), fct_cost AS (
  SELECT
    date_tzutc
    , app_name
    , media_source
    , ad_network
    , monetization_network
    , ad_format
    , event_name 
    , campaign
    , af_adset
    , af_ad
    , COUNT(app_name)                AS installs
    , is_organic
    , platform
    , country_code
    , source
    , COALESCE(SUM(af_cost_value), 0) AS cost_usd
    , est_revenue_usd
    , impressions
    , est_epcm
  FROM installs
  GROUP BY ALL

)

SELECT * FROM fct_cost

