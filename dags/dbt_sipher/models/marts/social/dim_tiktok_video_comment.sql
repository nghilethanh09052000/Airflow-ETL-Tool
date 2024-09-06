{{- config(
  partition_by={
    'field': 'snapshot_date_tzict',
    'data_type': 'date',
  },
  materialized='table'
) -}}


SELECT
  video_id,
  comment_text,
  comment_user_id,
  snapshot_date_tzict
FROM {{ ref('stg_tiktok_video_comment') }}
WHERE comment_text IS NOT NULL
