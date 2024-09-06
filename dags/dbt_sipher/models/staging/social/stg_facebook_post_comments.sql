{{- config(
    materialized='view',
)-}}

WITH final AS (
SELECT
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%z", created_time) AS created_time
  , post_id
  , id AS message_id
  , message
  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'facebook_sipher_post_comments') }}

)

SELECT * FROM final
