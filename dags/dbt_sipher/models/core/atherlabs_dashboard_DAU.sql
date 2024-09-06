{{config(materialized='table')}}

WITH a AS (
    SELECT 
      PARSE_DATE('%Y%m%d',ev.event_date) AS act_date
      , ev.user_id
      , ev.user_pseudo_id
      , co.country
    FROM `sipher-atherlabs-ga.analytics_341181237.events_*` ev
    LEFT JOIN {{ ref('dim_ather_user__country')}} co 
    ON ev.user_id = co.user_id
    WHERE ev.event_name IN ('session_start', 'user_engagement', 'page_view')
)

SELECT
  act_date
  , EXTRACT(MONTH FROM act_date) AS act_month
  , COUNT(DISTINCT user_id) AS user_id_cnt
  , COUNT(DISTINCT user_pseudo_id) AS user_pseudo_id_cnt
  , country
FROM a
GROUP BY 1,5
