CREATE OR REPLACE TABLE `sipherg1production.snapshot.events_snapshot`
AS

SELECT
  *,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS _collected_timestamp,
  _TABLE_SUFFIX AS __table_suffix
FROM `sipherg1production.analytics_387396350.events_*`
WHERE PARSE_DATE('%Y%m%d', SUBSTR(_TABLE_SUFFIX, -8)) BETWEEN DATE_SUB("{{ tomorrow_ds }}" , INTERVAL 7 DAY) AND "{{ tomorrow_ds }}"
