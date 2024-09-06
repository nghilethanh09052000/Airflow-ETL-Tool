WITH 
rankings_all_day AS(
    SELECT
      today.app_id AS app_id,
      today.os AS os,
      today.ranking AS today_rank,
      yesterday.ranking AS yesterday_rank,
      CASE
        WHEN today.ranking > yesterday.ranking THEN CAST(today.ranking-yesterday.ranking AS STRING)
        WHEN yesterday.ranking > today.ranking THEN CAST(yesterday.ranking-today.ranking AS STRING)
        WHEN yesterday.ranking = today.ranking THEN '0'
        ELSE ''
      END AS rank_change,
      CASE
        WHEN today.ranking IS NULL THEN 'OUT OF TOP 50'
        WHEN yesterday.ranking IS NULL THEN 'DROP ~50 RANK'
        WHEN today.ranking > yesterday.ranking THEN 'drop'
        WHEN yesterday.ranking > today.ranking THEN 'up'
        WHEN yesterday.ranking = today.ranking THEN 'same_rank'
      END AS rank_status
    FROM `sipher-data-platform.raw_sensortower.daily_top_apps_{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}` today
    FULL OUTER JOIN `sipher-data-platform.raw_sensortower.daily_top_apps_{{ macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d") }}` yesterday ON today.app_id = yesterday.app_id AND today.os = yesterday.os
    
)

SELECT DISTINCT
  rankings_all_day.app_id AS app_id,
  CONCAT(meta.name, '_', rankings_all_day.os) AS app_name,
  rankings_all_day.os AS os,
  today_rank,
  rank_change,
  CASE
    WHEN rank_status = 'drop' OR rank_status = 'up' THEN CONCAT(rank_status,'_',rank_change,'_','rank')
    ELSE rank_status
  END AS rank_status
FROM rankings_all_day 
LEFT JOIN `sipher-data-platform.raw_sensortower.app_metadata` AS meta ON meta.app_id = rankings_all_day.app_id 
WHERE today_rank <= 50 AND SAFE_CAST(rank_change AS INT64) >=5
ORDER BY rankings_all_day.os, CAST(today_rank AS INT64), rank_change