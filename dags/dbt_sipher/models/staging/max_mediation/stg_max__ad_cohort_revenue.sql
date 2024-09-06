{{- config(
    materialized = 'view',
)-}}

SELECT
  PARSE_DATE('%Y-%m-%d', day)  AS date_tzutc
  , platform
  , UPPER(country) AS country
  , CAST(SAFE_CAST   (installs   AS float64) AS int64) AS installs
  , CAST(reward_pub_revenue_0    AS float64)           AS reward_pub_revenue_0
  , CAST(reward_pub_revenue_1    AS float64)           AS reward_pub_revenue_1
  , CAST(reward_pub_revenue_2    AS float64)           AS reward_pub_revenue_2
  , CAST(reward_pub_revenue_3    AS float64)           AS reward_pub_revenue_3
  , CAST(reward_pub_revenue_7    AS float64)           AS reward_pub_revenue_7
  , CAST(reward_pub_revenue_14   AS float64)           AS reward_pub_revenue_14
  , CAST(reward_pub_revenue_30   AS float64)           AS reward_pub_revenue_30
  , CAST(banner_pub_revenue_0    AS float64)           AS banner_pub_revenue_0
  , CAST(banner_pub_revenue_1    AS float64)           AS banner_pub_revenue_1
  , CAST(banner_pub_revenue_2    AS float64)           AS banner_pub_revenue_2
  , CAST(banner_pub_revenue_3    AS float64)           AS banner_pub_revenue_3
  , CAST(banner_pub_revenue_7    AS float64)           AS banner_pub_revenue_7
  , CAST(banner_pub_revenue_14   AS float64)           AS banner_pub_revenue_14
  , CAST(banner_pub_revenue_30   AS float64)           AS banner_pub_revenue_30
  , CAST(inter_pub_revenue_0     AS float64)           AS inter_pub_revenue_0
  , CAST(inter_pub_revenue_1     AS float64)           AS inter_pub_revenue_1
  , CAST(inter_pub_revenue_2     AS float64)           AS inter_pub_revenue_2
  , CAST(inter_pub_revenue_3     AS float64)           AS inter_pub_revenue_3
  , CAST(inter_pub_revenue_7     AS float64)           AS inter_pub_revenue_7
  , CAST(inter_pub_revenue_14    AS float64)           AS inter_pub_revenue_14
  , CAST(inter_pub_revenue_30    AS float64)           AS inter_pub_revenue_30
FROM 
  {{ source('raw_max_mediation', 'raw_cohort_ad_revenue_perpormance') }}
