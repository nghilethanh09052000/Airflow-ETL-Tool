{{- config(
  materialized='view',
) -}}

{%- set latest_table_suffix = get_latest_table_suffix(
    table=source('raw_social', 'twitter_user_info')
) -%}

SELECT
  id,
  username,
  name,
  created_at AS user_created_at,
  description,
  verified AS is_verified,
  location AS user_defined_location,
  profile_image_url,
  url,
  entities.description.cashtags,
  entities.description.hashtags,
  entities.description.mentions,
  entities.description.urls AS entities_urls,
  entities.url.urls,
  protected AS is_protected,
  pinned_tweet_id,

  public_metrics.followers_count AS followers_cnt,
  public_metrics.following_count AS following_cnt,
  public_metrics.listed_count AS listed_cnt,
  public_metrics.tweet_count AS tweet_cnt
FROM {{ source('raw_social', 'twitter_user_info') }}
WHERE _TABLE_SUFFIX = "{{ latest_table_suffix }}"
