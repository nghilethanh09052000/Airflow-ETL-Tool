{{- config(
  materialized='view'
) -}}


SELECT
  id,
  owner_id,
  name,
  description,
  owner,
  member_count,
  created_at AS profile_created_at,
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS snapshot_date_tzict
FROM {{ source('raw_social', 'discord_profile_stats') }}
