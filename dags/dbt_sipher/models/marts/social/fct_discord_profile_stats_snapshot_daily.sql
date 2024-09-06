{{- config(
  partition_by={
    'field': 'snapshot_date_tzict',
    'data_type': 'date',
  },
  materialized='table'
) -}}

SELECT
  snapshot_date_tzict,
  id,
  member_count
FROM {{ ref('stg_discord_profile_stats_snapshot') }}
