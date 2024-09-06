{{- config(
  materialized='view'
) -}}


SELECT
  *,
  _TABLE_SUFFIX AS __table_suffix
FROM {{ source('raw_firebase_hidden_atlas', 'events') }}
