{{- config(
  materialized='view'
) -}}


SELECT
  PARSE_DATE('%Y%m%d', date) AS snapshot_date_tzict,
  followers AS followers_cnt,
  hearts AS hearts_cnt,
  diggs AS diggs_cnt,
  videos AS videos_cnt
FROM {{ source('raw_social', 'tiktok_profile_stats') }}
