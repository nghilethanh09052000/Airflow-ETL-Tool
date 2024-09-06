{{- config(
  materialized='table'
)-}}

WITH fact AS (
  SELECT
    *
    , {{ get_string_value_from_event_params(key="id") }} AS image_id
  FROM {{ ref("fct_artventure_image_events") }}
  WHERE event_name = 'view'
)

,dim AS (
  SELECT
    *
  FROM {{ ref("dim_artventure_image") }}
)

,image_stats AS (
  SELECT
    DISTINCT image_id
    , version
    , COUNT(*) AS view_count
    , MAX({{ get_int_value_from_event_params(key="comment_count") }})  AS comment_count
    , MAX({{ get_int_value_from_event_params(key="download_count") }}) AS download_count
    , MAX({{ get_int_value_from_event_params(key="favorite_count") }}) AS favorite_count
    , MAX(timestamp) AS timestamp
    , MAX(date) AS date
  FROM fact
  GROUP BY image_id, version
)

,mart AS (
  SELECT
    DISTINCT image_id
    , image_name
    , image_size
    , image_width
    , image_height
    , view_count
    , comment_count
    , download_count
    , favorite_count
    , image_owner_uid
    , i.version
    , COALESCE(uploaded_at,timestamp) AS uploaded_at
    , COALESCE(dim.date, i.date) AS date
  FROM image_stats i
    LEFT JOIN dim USING(image_id)
)

SELECT * FROM mart