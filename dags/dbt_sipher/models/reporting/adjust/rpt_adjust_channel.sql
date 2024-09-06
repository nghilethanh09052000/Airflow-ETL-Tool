{{- config(
  materialized='table',
  partition_by={
    "field": "created_at_date",
    "data_type": "date",
    "granularity": "day"
},
) -}}

WITH data AS (
  SELECT
    created_at_date
    , app_name_dashboard
    , environment
    , os_name
    , device_type
    , network_name
    , SUM(CASE WHEN activity_kind = 'impression'           THEN count END ) AS impressions
    , SUM(CASE WHEN activity_kind = 'click'                THEN count END ) AS clicks
    , SUM(CASE WHEN activity_kind = 'install'              THEN count END ) AS installs
    , SUM(CASE WHEN activity_kind = 'install_update'       THEN count END ) AS install_update
    , SUM(CASE WHEN activity_kind = 'event'                THEN count END ) AS events
    , SUM(CASE WHEN activity_kind = 'reattribution'        THEN count END ) AS reattributions
    , SUM(CASE WHEN activity_kind = 'reattribution_update' THEN count END ) AS reattribution_update
    , SUM(CASE WHEN activity_kind = 'session'              THEN count END ) AS sessions
    , COUNT(DISTINCT user_id) AS dau
    , SUM(revenue) AS revenue
    , SUM(cost) AS cost
  FROM 
    {{ ref('fct_adjust')}}
  GROUP BY 1,2,3,4,5,6
)
, c AS (
  SELECT
    created_at_date
    , app_name_dashboard
    , environment
    , os_name
    , device_type
    , network_name
    , COALESCE(impressions,0)          AS impressions
    , COALESCE(clicks,0)               AS clicks
    , COALESCE(CASE WHEN clicks IS NOT NULL AND impressions IS NOT NULL THEN (clicks / impressions)*100  END,0) AS CTR
    , COALESCE(installs,0)             AS installs
    , COALESCE(install_update,0)       AS install_update
    , COALESCE(CASE WHEN clicks IS NOT NULL AND installs    IS NOT NULL THEN (installs / clicks)*100     END,0) AS CCR
    , COALESCE(events,0)               AS events
    , COALESCE(reattributions,0)       AS reattributions
    , COALESCE(reattribution_update,0) AS reattribution_update
    , COALESCE(sessions,0)             AS sessions
    , COALESCE(dau,0)                  AS dau
    , COALESCE(revenue,0)              AS revenue
    , COALESCE(cost,0)                 AS cost
  FROM data

)

SELECT * FROM c