{{- config(
  materialized='incremental',
  unique_key='post_id',
  merge_update_columns=['latest_update_at']
) -}}

WITH posts AS (
  SELECT
    created_time
    , post_id
    , page_id
    , MAX(updated_time) AS latest_update_at
    , message
    , permalink_url AS post_url
    , author_id
    , author_name
    , full_picture_url
    , instagram_eligibility
    , is_eligible_for_promotion
    , is_expired
    , is_hidden
    , is_instagram_eligible
    , is_popular
    , is_published
    , is_spherical
    , privacy_allow
    , privacy_deny
    , privacy_description
    , privacy_friends
    , privacy_value
    , status_type
    , subscribed
    , promotable_id
    , video_buying_eligibility
    , properties_name
    , properties_value
  FROM 
    {{ ref('stg_facebook_page_feed') }}
  GROUP BY 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
, fi AS (
  SELECT 
    * 
    , ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY post_id, latest_update_at) AS row_number
  FROM posts
)
, final AS (
  SELECT * EXCEPT(row_number)
  FROM fi
  WHERE row_number = 1

)

SELECT * FROM final

