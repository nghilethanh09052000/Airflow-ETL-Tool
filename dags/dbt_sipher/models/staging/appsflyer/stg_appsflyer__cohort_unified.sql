{{- config(
    materialized = 'view',
)-}}

SELECT
    app_id
    ,conversion_type
    ,attributed_touch_type
    ,CAST(days_post_attribution AS INT64)       AS days_post_attribution
    ,CAST(conversion_date       AS TIMESTAMP)   AS conversion_date
    ,CAST(event_date            AS TIMESTAMP)   AS event_date
    ,event_name
    ,media_source
    ,campaign
    ,campaign_id
    ,adset                                AS af_adset
    ,adset_id                             AS af_adset_id
    ,ad                                   AS af_ad
    ,ad_id                                AS af_ad_id
    ,channel
    ,site_id                              AS af_site_id
    ,UPPER(geo)                           AS geo
    ,CAST(unique_users AS INT64)          AS unique_users
    ,CAST(event_count  AS INT64)          AS event_count
    ,CAST(revenue_usd  AS FLOAT64)        AS revenue_usd
    ,PARSE_DATE('%Y-%m-%d', dt)           AS date_tzutc
FROM {{source('raw_appsflyer_locker','cohort_unified')}}