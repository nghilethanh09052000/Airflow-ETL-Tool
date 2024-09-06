{{- config(
    materialized = 'view',
)-}}

SELECT
    attributed_touch_type
    ,CAST(attributed_touch_time AS TIMESTAMP )             AS attributed_touch_time
    ,CAST(install_time          AS TIMESTAMP )             AS install_time
    ,CAST(event_time            AS TIMESTAMP )             AS event_time
    ,event_name
    ,CAST(event_revenue_usd     AS FLOAT64)                AS event_revenue_usd
    ,media_source
    ,af_channel
    ,campaign
    ,af_adset
    ,af_adset_id
    ,af_ad
    ,af_ad_id
    ,af_siteid
    ,UPPER(region)                                         AS region
    ,UPPER(country_code)                                   AS country_code
    ,state
    ,city
    ,appsflyer_id
    ,advertising_id
    ,UPPER(platform)                                       AS platform
    ,app_id
    ,app_name
    ,bundle_id
    ,mediation_network
    ,CAST(REGEXP_EXTRACT(impressions, r'^(\d+)') AS int64) AS impressions   
    ,monetization_network
    ,is_organic
    ,device_id_type
    ,PARSE_DATE('%Y-%m-%d', dt)                            AS date_tzutc
    ,CAST(h AS INT64)                                      AS hour
FROM {{ source('raw_appsflyer_locker', 'organic_ad_revenue') }}