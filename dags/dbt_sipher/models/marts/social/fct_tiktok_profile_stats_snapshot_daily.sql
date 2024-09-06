{{- config(
  partition_by={
    'field': 'snapshot_date_tzict',
    'data_type': 'date',
  },
  materialized='table'
) -}}


SELECT
  *
FROM {{ ref('stg_tiktok_profile_stats') }}
