{{- config(
  materialized='view',
) -}}


SELECT
  id,
  username,
  name,
  created_at AS profile_created_at,
  description,
  protected AS is_protected,
  verified AS is_verified,
  location AS user_defined_location,
  url AS user_defined_url,
  pinned_tweet_id,
  profile_image_url,
  entities.url.urls,
  public_metrics.followers_count AS followers_cnt,
  public_metrics.following_count AS following_cnt,
  public_metrics.listed_count AS listed_cnt,
  public_metrics.tweet_count AS tweet_cnt,
  __collected_ts,
  snapshot_date
FROM {{ source('raw_social', 'twitter_profile_stats') }}
