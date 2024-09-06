{{config(materialized='table')}}

SELECT
  EXTRACT(MONTH FROM act_date) AS act_month,
  COUNT(DISTINCT user_id) AS user_id_cnt,
  COUNT(DISTINCT user_pseudo_id) AS user_pseudo_id_cnt,

FROM `sipher-atherlabs-ga.analytics_341181237.events_*` 
WHERE event_name IN ('session_start', 'user_engagement', 'page_view')
GROUP BY 1
