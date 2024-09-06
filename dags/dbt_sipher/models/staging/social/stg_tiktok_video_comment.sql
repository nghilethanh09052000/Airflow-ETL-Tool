{{- config(
  materialized='view'
) -}}


SELECT
  post_id AS video_id,
  `desc` AS video_description,
  music AS video_music,
  comments AS comments_cnt,
  diggs AS diggs_cnt,
  download AS download_cnt,
  play AS play_cnt,
  forward AS forward_cnt,
  share AS share_cnt,
  comment_text,
  comment_user_id,
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS snapshot_date_tzict
FROM {{ source('raw_social', 'tiktok_video_comment') }}
