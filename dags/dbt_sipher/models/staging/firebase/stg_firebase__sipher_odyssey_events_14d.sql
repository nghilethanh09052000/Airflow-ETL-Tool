{{- config(
  materialized='view'
) -}}


SELECT
  *,
  _TABLE_SUFFIX AS __table_suffix
FROM {{ source('raw_firebase_sipher_odyssey', 'events') }}
WHERE PARSE_DATE('%Y%m%d', SUBSTR(_TABLE_SUFFIX, -8)) BETWEEN DATE_SUB(CURRENT_DATE() , INTERVAL 14 DAY) AND CURRENT_DATE()
