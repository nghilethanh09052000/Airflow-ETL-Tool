{{- config(
  partition_by={
    'field': 'snapshot_date_tzict',
    'data_type': 'date',
  },
  materialized='table'
) -}}


WITH
discord_profile AS
  (SELECT
    fct.snapshot_date_tzict,
    'Discord' AS channel,
    COALESCE(fct.id, dim.id) AS id,
    name,

    member_count AS discord_member_cnt,

    CAST(NULL AS INT64) AS tiktok_followers_cnt,
    CAST(NULL AS INT64) AS tiktok_hearts_cnt,
    CAST(NULL AS INT64) AS tiktok_diggs_cnt,
    CAST(NULL AS INT64) AS tiktok_videos_cnt,

    CAST(NULL AS INT64) AS twitter_followers_cnt,
    CAST(NULL AS INT64) AS twitter_following_cnt,
    CAST(NULL AS INT64) AS twitter_listed_cnt,
    CAST(NULL AS INT64) AS twitter_tweet_cnt
  FROM {{ ref('fct_discord_profile_stats_snapshot_daily') }} AS fct
  LEFT JOIN {{ ref('dim_discord_profile') }} AS dim
    ON fct.id = dim.id)

,tiktok_profile AS
  (SELECT
    snapshot_date_tzict,
    'Tiktok' AS channel,
    CAST(NULL AS INT64) AS id,
    CAST(NULL AS STRING) AS name,

    CAST(NULL AS INT64) AS discord_member_cnt,

    followers_cnt AS tiktok_followers_cnt,
    hearts_cnt AS tiktok_hearts_cnt,
    diggs_cnt AS tiktok_diggs_cnt,
    videos_cnt AS tiktok_videos_cnt,

    CAST(NULL AS INT64) AS twitter_followers_cnt,
    CAST(NULL AS INT64) AS twitter_following_cnt,
    CAST(NULL AS INT64) AS twitter_listed_cnt,
    CAST(NULL AS INT64) AS twitter_tweet_cnt
  FROM {{ ref('fct_tiktok_profile_stats_snapshot_daily') }} AS fct)

,twitter_profile AS
  (SELECT
    fct.snapshot_date_tzict,
    'Twitter' AS channel,
    COALESCE(fct.id, dim.id) AS id,
    name,

    CAST(NULL AS INT64) AS discord_member_cnt,

    CAST(NULL AS INT64) AS tiktok_followers_cnt,
    CAST(NULL AS INT64) AS tiktok_hearts_cnt,
    CAST(NULL AS INT64) AS tiktok_diggs_cnt,
    CAST(NULL AS INT64) AS tiktok_videos_cnt,

    followers_cnt AS twitter_followers_cnt,
    following_cnt AS twitter_following_cnt,
    listed_cnt AS twitter_listed_cnt,
    tweet_cnt AS twitter_tweet_cnt
  FROM {{ ref('fct_twitter_profile_stats_snapshot_daily') }} AS fct
  LEFT JOIN {{ ref('dim_twitter_profile') }} AS dim
    ON fct.id = dim.id)

,final AS
  (SELECT * FROM discord_profile
  UNION ALL
  SELECT * FROM tiktok_profile)


SELECT * FROM final