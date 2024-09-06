{{- config(
  materialized='view'
) -}}


SELECT
  id,
  unique_id AS user_name,
  nickname,
  region AS country_code
FROM {{ source('raw_social', 'tiktok_user_info') }}
