CREATE OR REPLACE TABLE `{{ params.bq_project }}.raw_sensortower.daily_top_apps_{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}` AS

WITH 
ios_today AS(
  SELECT
    app_id,
    SUM(CAST(current_units_value AS INT64)) AS downloads,
    MAX(snapshot_date) AS snapshot_date,
    'ios' AS os
  FROM `sipher-data-platform.raw_sensortower.raw_store_intelligence_sales_report_estimates_comparison_attributes`
  WHERE 
  DATE(snapshot_date) BETWEEN DATE_SUB(DATE('{{ macros.ds_add(ds, -1) }}') , INTERVAL 3 DAY) AND DATE('{{ macros.ds_add(ds, -1) }}')
  AND SAFE_CAST(app_id AS INT64) IS NOT NULL
  AND game_class = 'true'
  GROUP BY app_id, os

  )

, android_today AS(
  SELECT
    app_id,
    SUM(CAST(current_units_value AS INT64)) AS downloads,
    MAX(snapshot_date) AS snapshot_date,
    'android' AS os,
  FROM `sipher-data-platform.raw_sensortower.raw_store_intelligence_sales_report_estimates_comparison_attributes`
  WHERE
  DATE(snapshot_date) BETWEEN DATE_SUB(DATE('{{ macros.ds_add(ds, -1) }}') , INTERVAL 3 DAY) AND DATE('{{ macros.ds_add(ds, -1) }}')
  AND REGEXP_CONTAINS(app_id, r'\.')
  AND game_class = 'true'
  GROUP BY app_id, os

)


SELECT 
*, 
  ROW_NUMBER() OVER (ORDER BY downloads DESC) AS ranking
FROM android_today
UNION ALL
SELECT 
*, 
  ROW_NUMBER() OVER (ORDER BY downloads DESC) AS ranking
FROM ios_today
