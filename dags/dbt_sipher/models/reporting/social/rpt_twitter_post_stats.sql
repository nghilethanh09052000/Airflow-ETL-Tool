{{- config(
    alias='twitter_post_stats',
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
)-}}

WITH
  twitter_timeline AS (
    SELECT
      *,
      (SELECT rt_list.item.type FROM UNNEST(referenced_tweets.list) AS rt_list ORDER BY 1 NULLS FIRST LIMIT 1) AS post_type
    FROM {{ ref("stg_twitter_timeline") }}
  )

  ,twitter_profile AS (
    SELECT * FROM {{ ref("dim_twitter_profile") }}
  )

SELECT
  created_at AS post_created_at,
  DATE(created_at) AS date,
  COALESCE(profile.username, "AtherLabs") AS author_user_name,
  text,
  impression_count AS impressions,
  like_count AS likes,
  reply_count AS replies,
  retweet_count AS retweet,
  quote_count AS quotes
FROM twitter_timeline AS timeline
LEFT JOIN twitter_profile AS profile
  ON timeline.author_id = profile.id
WHERE post_type IS NULL
