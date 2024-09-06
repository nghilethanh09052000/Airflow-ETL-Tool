{{- config(
  materialized='table'
) -}}

WITH all_images AS (
  SELECT
    DISTINCT {{ get_string_value_from_event_params(key="img_id") }} AS image_id,
    {{ get_string_value_from_event_params(key="img_name") }}        AS image_name,
    {{ get_string_value_from_event_params(key="img_owner_uid") }}   AS image_owner_uid,
    {{ get_double_value_from_event_params(key="img_size") }}        AS image_size,
    COALESCE(
      CAST({{ get_int_value_from_event_params(key="img_width") }}   AS STRING),
      {{ get_string_value_from_event_params(key="img_width") }}
    ) AS image_width,
    COALESCE(
      CAST({{ get_int_value_from_event_params(key="img_height") }}  AS STRING),
      {{ get_string_value_from_event_params(key="img_height") }}
    ) AS image_height,
    version,
    timestamp AS uploaded_at,
    date 
  FROM {{ ref("fct_artventure_image_events") }}
  WHERE event_name IN (
    'upload_img_success',
    'view'
  )
)

,final AS (
  SELECT
    image_id,
    image_name,
    image_owner_uid,
    image_size,
    image_width,
    image_height,
    version,
    uploaded_at,
    date
  FROM all_images
  WHERE image_id IS NOT NULL OR image_name IS NOT NULL
)

SELECT * FROM final