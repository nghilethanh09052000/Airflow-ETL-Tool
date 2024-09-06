{{- config(
  materialized='incremental',
  unique_key='id',
  merge_update_columns = ['name', 'description'],
) -}}

{%- set date_filter = get_max_column_value(
    table=ref('stg_discord_profile_stats_snapshot'),
    column='snapshot_date_tzict'
) -%}

SELECT DISTINCT
  id,
  name,
  profile_created_at,
  description,
  owner,
  owner_id
FROM {{ ref('stg_discord_profile_stats_snapshot') }}
WHERE snapshot_date_tzict = '{{ date_filter }}'
