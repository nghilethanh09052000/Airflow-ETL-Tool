{{- config(
  materialized='table',
) -}}

WITH devices AS (
  SELECT
    game_user_id,
    ather_id,
    ARRAY_AGG(STRUCT(
      last_used_at,
      advertising_ids,
      vendor_ids,
      user_pseudo_ids,
      category,
      mobile_brand_name,
      mobile_model_name,
      mobile_marketing_name,
      mobile_os_hardware_model,
      operating_system,
      operating_system_version,
      browser,
      browser_version,
      web_info_browser,
      web_info_browser_version,
      web_info_hostname
    )) AS device
  FROM {{ ref('int_sipher_odyssey_player_devices') }}
  GROUP BY game_user_id, ather_id
)

SELECT * FROM devices