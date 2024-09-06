{{- config(
    materialized = 'view',
)-}}

SELECT
  PARSE_DATE('%Y-%m-%d', day)         AS date_tzutc
  ,UPPER(country)                     AS country_code
  ,application                        AS app_name
  ,package_name                       AS bundle_id
  ,network
  ,network_placement
  ,max_ad_unit
  ,max_ad_unit_id
  ,ad_unit_waterfall_name
  ,device_type
  ,ad_format
  ,CAST(attempts          AS INT64)   AS attempts
  ,CAST(responses         AS INT64)   AS responses
  ,CAST(impressions       AS INT64)   AS impressions
  ,CAST(ecpm              AS FLOAT64) AS ecpm
  ,CAST(estimated_revenue AS FLOAT64) AS estimated_revenue
  ,store_id
  ,CASE
    WHEN SAFE_CAST(store_id AS INT64) IS NOT NULL THEN 'IOS'
    ELSE 'ANDROID'
  END AS platform
FROM {{ source('raw_max_mediation', 'raw_ad_revenue') }}