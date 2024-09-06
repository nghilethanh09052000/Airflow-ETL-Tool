{{- config(
    materialized = 'view',
)-}}

SELECT
    attributed_touch_type
    ,CAST(attributed_touch_time AS TIMESTAMP ) AS attributed_touch_time
    ,CAST(install_time          AS TIMESTAMP ) AS install_time
    ,CAST(event_time            AS TIMESTAMP ) AS event_time
    ,event_name
    ,af_cost_model
    ,CAST(af_cost_value         AS FLOAT64)    AS af_cost_value
    ,af_cost_currency
    ,media_source
    ,af_channel
    ,campaign
    ,af_adset
    ,af_adset_id
    ,af_ad
    ,af_ad_id
    ,af_siteid
    ,UPPER(region) AS region
    ,UPPER(country_code)                       AS country_code
    ,state
    ,city
    ,appsflyer_id
    ,advertising_id
    ,UPPER(platform)                           AS platform
    ,app_id
    ,app_name
    ,bundle_id
    ,mediation_network
    ,monetization_network
    ,is_organic
    ,device_id_type
    ,PARSE_DATE('%Y-%m-%d', dt)                AS date_tzutc
    ,CAST(h AS INT64)                          AS hour
FROM {{ source('raw_appsflyer_locker', 'installs') }}