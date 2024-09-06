{{- config(
    materialized='view',
)-}}

WITH final AS (
SELECT
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%z", end_time) AS end_time
  , page_id
  , name  AS metric_name
  , title AS metric_title
  , period
  , description
  , metric
  , reach AS value
  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'facebook_sipher_page_impression_gender_locate') }}

)

SELECT * FROM final