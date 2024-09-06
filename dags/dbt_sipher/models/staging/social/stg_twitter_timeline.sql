{{- config(
    alias='twitter_timeline',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
)-}}

WITH data AS (
    SELECT *
    FROM {{ source('raw_social', 'twitter_timeline') }}
    {% if is_incremental() -%}
    WHERE `snapshot_date` = '{{ var("snapshot_date", "2023-12-25") }}'
    {% endif -%}
    QUALIFY __collected_ts = MAX(__collected_ts) OVER (PARTITION BY account, created_at)
)


SELECT
  created_at,
  author_id,
  id,
  text,

  referenced_tweets,
  in_reply_to_user_id,
  reply_settings,
  conversation_id,
  lang,
  entities.annotations,
  entities.urls,
  attachments.media_keys,
  context_annotations,

  public_metrics.impression_count,
  public_metrics.like_count,
  public_metrics.reply_count,
  public_metrics.retweet_count,
  public_metrics.quote_count,

  __collected_ts,
  snapshot_date

FROM data
