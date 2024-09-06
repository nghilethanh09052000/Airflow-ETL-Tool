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
  followers_cnt,
  following_cnt,
  listed_cnt,
  tweet_cnt
FROM {{ ref('stg_twitter_profile_stats') }}
