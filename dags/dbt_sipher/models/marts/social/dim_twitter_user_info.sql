{{- config(
  materialized='incremental',
  unique_key='id',
) -}}

SELECT
  id,
  username,
  name,
  user_created_at,
  description,
  is_verified,
  user_defined_location,
  profile_image_url,
  url,
  cashtags,
  hashtags,
  mentions,
  entities_urls,
  urls,
  is_protected,
  pinned_tweet_id,
  followers_cnt,
  following_cnt,
  listed_cnt,
  tweet_cnt
FROM {{ ref('stg_twitter_user_info_latest_snapshot') }}
