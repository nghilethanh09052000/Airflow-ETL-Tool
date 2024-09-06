{{- config(
  materialized='incremental',
  unique_key='user_name',
  merge_update_columns = ['id', 'nickname', 'country_code'],
) -}}


SELECT * FROM {{ ref('stg_tiktok_user_info') }}
