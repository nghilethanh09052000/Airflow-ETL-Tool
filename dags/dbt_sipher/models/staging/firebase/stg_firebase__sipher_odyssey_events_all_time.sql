{{- config(
  materialized='view'
) -}}


SELECT
  *,
  _TABLE_SUFFIX AS __table_suffix
FROM {{ source('raw_firebase_sipher_odyssey', 'events') }}
WHERE _TABLE_SUFFIX >= '20240308'
