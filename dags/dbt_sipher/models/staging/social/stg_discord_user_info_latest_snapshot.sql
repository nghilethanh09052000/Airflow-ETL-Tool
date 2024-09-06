{{- config(
  materialized='view'
) -}}

{%- set latest_table_suffix = get_latest_table_suffix(
    table=source('raw_social', 'discord_user_info')
) -%}


SELECT
  SAFE_CAST(id AS INT64) AS id,
  SAFE_CAST(name AS STRING) AS name,
  SAFE_CAST(nick AS STRING) AS nick,

  SAFE_CAST(bot AS BOOLEAN) AS is_bot,
  SAFE_CAST(pending AS BOOLEAN) AS is_pending,
  SAFE_CAST(system AS BOOLEAN) AS is_system,

  SAFE_CAST(activities AS STRING) AS activities,
  SAFE_CAST(discriminator AS INT64) AS discriminator,
  SAFE_CAST(guild AS STRING) AS guild,
  SAFE_CAST(guild_permissions AS STRING) AS guild_permissions,
  SAFE_CAST(mention AS STRING) AS mention,
  SAFE_CAST(public_flags AS STRING) AS public_flags,
  SAFE_CAST(raw_status AS STRING) AS raw_status,
  SAFE_CAST(status AS STRING) AS status,
  SAFE_CAST(web_status AS STRING) AS web_status,
  SAFE_CAST(desktop_status AS STRING) AS desktop_status,
  SAFE_CAST(mobile_status AS STRING) AS mobile_status,
  SAFE_CAST(roles AS STRING) AS roles,
  SAFE_CAST(top_role AS STRING) AS top_role,
  SAFE_CAST(voice AS STRING) AS voice,

  SAFE_CAST(premium_since AS TIMESTAMP) AS premium_since,
  SAFE_CAST(created_at AS TIMESTAMP) AS user_created_at,
  SAFE_CAST(joined_at AS TIMESTAMP) AS joined_sipher_guild_at,
FROM {{ source('raw_social', 'discord_user_info') }}
WHERE _TABLE_SUFFIX = "{{ latest_table_suffix }}"