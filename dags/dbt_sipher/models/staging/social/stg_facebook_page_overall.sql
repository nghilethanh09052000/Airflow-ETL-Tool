{{- config(
    materialized='view',
)-}}

WITH final AS (
SELECT
  page_id
  , name AS page_name
  , username
  , category
  , followers_count
  , is_community_page
  , link
  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'facebook_sipher_page_overall') }}

)

SELECT * FROM final