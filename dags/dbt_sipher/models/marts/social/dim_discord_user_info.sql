{{- config(
  materialized='incremental',
  unique_key='id',
  merge_update_columns = ['name', 'status', 'roles'],
) -}}


SELECT * FROM {{ ref('stg_discord_user_info_latest_snapshot') }}
