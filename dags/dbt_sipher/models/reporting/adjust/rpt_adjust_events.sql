{{- config(
  materialized='table',
  partition_by={
    "field": "created_at_date",
    "data_type": "date",
    "granularity": "day"
},
) -}}

WITH ev AS (
    SELECT  
        created_at_date
        , app_name_dashboard
        , app_version
        , environment
        , os_name
        , device_type
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'in_app_purchase'          THEN count END),0) AS in_app_purchase
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ads_impression'           THEN count END),0) AS ads_impression
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_rewarded_displayed'    THEN count END),0) AS ad_rewarded_displayed
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_rewarded_ad_completed' THEN count END),0) AS ad_rewarded_ad_completed
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_rewarded_ad_eligible'  THEN count END),0) AS ad_rewarded_ad_eligible
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_reward_shown_failed'   THEN count END),0) AS ad_reward_shown_failed
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_rewarded_api_called'   THEN count END),0) AS ad_rewarded_api_called
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_inters_api_called'     THEN count END),0) AS ad_inters_api_called
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_inters_displayed'      THEN count END),0) AS ad_inters_displayed
        , COALESCE(SUM(CASE WHEN activity_kind = 'event' AND event_name = 'ad_inters_ad_eligible'    THEN count END),0) AS ad_inters_ad_eligible
    FROM 
        {{ ref('fct_adjust')}} 
    GROUP BY ALL

)

SELECT * FROM ev