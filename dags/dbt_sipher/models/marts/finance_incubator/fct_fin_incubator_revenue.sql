{{- config(
    materialized = 'table',
    partition_by={
		'field': 'date_tzutc',
		'data_type': 'DATE',
	},
)-}}

WITH max_ad AS (
  SELECT 
    date_tzutc
    , CASE
        WHEN app_name = 'watermelon.suika2048.fruit.merge.drop' THEN 'Watermelon 2048: Merge Fruits'
        ELSE app_name
    END AS app_name
    , network
    , '' AS media_source
    , ad_format
    , '' AS event_name
    , '' AS campaign
    , '' AS af_adset
    , '' AS af_ad
    , 'false' AS is_organic
    , platform
    , country_code
    , ''              AS monetization_network
    , 'max_mediation' AS source
    , 0               AS cost_usd
    , estimated_revenue
    , impressions
  FROM {{ ref('stg_max__ad_revenue')}}
), fct_max AS (
  SELECT 
    date_tzutc
    , app_name
    , media_source
    , network                           AS ad_network
    , monetization_network
    , ad_format
    , event_name
    , campaign
    , af_adset
    , af_ad
    , is_organic
    , platform
    , country_code
    , source
    , cost_usd
    , SUM     (estimated_revenue      ) AS est_revenue_usd
    , SUM     (impressions            ) AS impressions
    , CASE
        WHEN SUM(impressions) = 0 THEN 0
        ELSE (SUM(estimated_revenue)*1000 / SUM(impressions))
    END AS est_epcm
  FROM max_ad
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
---appsflyer.attributed_ad_revenue
,att_ad_revenue AS (
  SELECT
    date_tzutc
    , CASE 
      WHEN app_id = 'id6462842931'                               THEN 'Hidden Atlas'
      WHEN app_id = 'hidden.atlas.seek.spot.objects.zoom.maps2d' THEN 'Hidden Atlas: Anime Zen Object'
      WHEN app_id = 'com.atherlabs.sipherodyssey'                THEN 'Sipher Odyssey'
      WHEN app_id = 'id6471829700'                               THEN 'Watermelon 2048: Merge Fruits'
      ELSE app_id
    END AS app_name
    , event_name
    , media_source
    , campaign
    , af_adset
    , af_ad
    , monetization_network
    , platform
    , is_organic
    , country_code
    , event_revenue_usd
    , impressions
    , 'appsflyer' AS source
    , ''          AS ad_format
    , ''          AS ad_network
    , 0           AS cost_usd
  FROM {{ ref('stg_appsflyer__attributed_ad_revenue')}}
), fct_att AS (
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
    , is_organic
    , platform
    , country_code
    , source
    , cost_usd
    , SUM     (event_revenue_usd      ) AS est_revenue_usd
    , SUM     (impressions            ) AS impressions
    , CASE
        WHEN SUM(impressions) = 0 THEN 0
        ELSE (SUM(event_revenue_usd)*1000 / SUM(impressions))
    END AS est_epcm
  FROM att_ad_revenue
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
----appsflyer.organic_ad_revenue
,organic_ad AS (
  SELECT
    date_tzutc
    , CASE 
      WHEN app_id = 'id6462842931'                               THEN 'Hidden Atlas'
      WHEN app_id = 'hidden.atlas.seek.spot.objects.zoom.maps2d' THEN 'Hidden Atlas: Anime Zen Object'
      WHEN app_id = 'com.atherlabs.sipherodyssey'                THEN 'Sipher Odyssey'
      WHEN app_id = 'id6471829700'                               THEN 'Watermelon 2048: Merge Fruits'
      ELSE app_id
    END AS app_name
    , event_name
    , media_source
    , campaign
    , af_adset
    , af_ad
    , monetization_network
    , platform
    , is_organic
    , country_code
    , event_revenue_usd
    , impressions
    , 'appsflyer' AS source
    , ''          AS ad_format
    , ''          AS ad_network
    , 0        AS cost_usd
  FROM {{ ref('stg_appsflyer__organic_ad_revenue')}}
  {# WHERE date_tzutc = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) #}
), fct_organic AS (
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
    , is_organic
    , platform
    , country_code
    , source
    , cost_usd
    , SUM     (event_revenue_usd      ) AS est_revenue_usd
    , SUM     (impressions            ) AS impressions
    , CASE
        WHEN SUM(impressions) = 0 THEN 0
        ELSE (SUM(event_revenue_usd)*1000 / SUM(impressions))
    END AS est_epcm
  FROM organic_ad
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
----appsflyer.inapps
,inapps AS (
  SELECT
    date_tzutc
    , CASE 
        WHEN app_id = 'id6462842931'                               THEN 'Hidden Atlas'
        WHEN app_id = 'hidden.atlas.seek.spot.objects.zoom.maps2d' THEN 'Hidden Atlas: Anime Zen Object'
        WHEN app_id = 'com.atherlabs.sipherodyssey'                THEN 'Sipher Odyssey'
        WHEN app_id = 'id6471829700'                               THEN 'Watermelon 2048: Merge Fruits'
        ELSE app_id
    END AS app_name
    , CASE
        WHEN event_name = 'game_af_purchase'                       THEN 'af_purchase'
        ELSE event_name
      END AS event_name
    , media_source
    , campaign
    , af_adset
    , af_ad
    , monetization_network
    , platform
    , is_organic
    , country_code
    , event_revenue_currency
    , event_revenue_usd
    , impressions
    , 'appsflyer' AS source
    , ''          AS ad_format
    , ''          AS ad_network
    , 0           AS cost_usd
  FROM {{ ref('stg_appsflyer__inapps')}}
), fct_inapps AS (
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
    , is_organic
    , platform
    , country_code
    , source
    , cost_usd
    , SUM     (event_revenue_usd      ) AS est_revenue_usd
    , SUM     (impressions            ) AS impressions
    , CASE
        WHEN SUM(impressions) = 0 THEN 0
        ELSE (SUM(event_revenue_usd)*1000 / SUM(impressions))
    END AS est_epcm
  FROM inapps  
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
), re AS (
  SELECT * FROM fct_max
  WHERE est_revenue_usd IS NOT NULL

  UNION ALL

  SELECT * FROM fct_att
  WHERE est_revenue_usd IS NOT NULL

  UNION ALL

  SELECT * FROM fct_organic
  WHERE est_revenue_usd IS NOT NULL

  UNION ALL

  SELECT * FROM fct_inapps
  WHERE est_revenue_usd IS NOT NULL
)

SELECT * FROM revenue