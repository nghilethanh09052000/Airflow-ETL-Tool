CREATE OR REPLACE TABLE `sipher-data-platform.reporting_game_product.raw_user_devices_{{ macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d") }}` AS
SELECT 
  DISTINCT 
  user_id,
  device.category,
  device.operating_system,
  device.operating_system_version,
  device.mobile_brand_name,
  device.mobile_model_name,
  device.mobile_marketing_name,
  geo.continent,
  geo.country,
  geo.region,
  geo.city,
  geo.sub_continent,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'build_number') AS build_number
 FROM `sipherg1production.analytics_387396350.events_{{ macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d") }}`
WHERE user_id IS NOT NULL