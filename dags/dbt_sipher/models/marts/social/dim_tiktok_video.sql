{{- config(
  materialized='incremental',
  unique_key='id',
  merge_update_columns = ['description', 'music'],
) -}}

{%- set date_filter = get_max_column_value(
    table=ref('stg_tiktok_video_comment'),
    column='snapshot_date_tzict'
) -%}

SELECT
  video_id AS id,
  MAX(video_description) AS description,
  MAX(video_music) AS music
FROM {{ ref('stg_tiktok_video_comment') }}
WHERE snapshot_date_tzict = '{{ date_filter }}'
GROUP BY id