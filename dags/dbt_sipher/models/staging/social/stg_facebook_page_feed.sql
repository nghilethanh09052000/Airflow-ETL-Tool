{{- config(
    materialized='view',
)-}}

WITH data AS (
SELECT 
  PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%z", created_time) AS created_time
  , page_id
  , id AS post_id
  , message
  , permalink_url
  , `from`.id    AS author_id
  , `from`.name  AS author_name
  , full_picture AS full_picture_url
  , instagram_eligibility
  , is_eligible_for_promotion
  , is_expired
  , is_hidden
  , is_instagram_eligible
  , is_popular
  , is_published
  , is_spherical
  , shares.count AS shares
  , privacy.allow       AS privacy_allow
  , privacy.deny        AS privacy_deny
  , privacy.description AS privacy_description
  , privacy.friends     AS privacy_friends
  , privacy.value       AS privacy_value
  , status_type
  , subscribed
  , promotable_id
  , PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%z", updated_time) AS updated_time
  , (SELECT vbe_list.element            FROM UNNEST(video_buying_eligibility.list) AS vbe_list)        AS video_buying_eligibility
  , (SELECT properties_list.element.name FROM UNNEST(properties.list)               AS properties_list) AS properties_name
  , (SELECT properties_list.element.text FROM UNNEST(properties.list)               AS properties_list) AS properties_value
  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'facebook_sipher_page_feed') }}

)

SELECT * FROM data

