{{- config(
    materialized='view',
)-}}

WITH final AS (
SELECT
  post_id
  , period
  , CAST(post_clicks                      AS INT64) AS post_clicks
  , CAST(post_clicks_unique               AS INT64) AS post_clicks_unique
  , CAST(post_engaged_fan                 AS INT64) AS post_engaged_fan
  , CAST(post_engaged_users               AS INT64) AS post_engaged_users
  , CAST(post_impressions                 AS INT64) AS post_impressions
  , CAST(post_impressions_fan             AS INT64) AS post_impressions_fan
--  , CAST(post_impressions_fan_paid        AS INT64) AS post_impressions_fan_paid
--  , CAST(post_impressions_fan_paid_unique AS INT64) AS post_impressions_fan_paid_unique
  , CAST(post_impressions_fan_unique      AS INT64) AS post_impressions_fan_unique
  , CAST(post_impressions_nonviral        AS INT64) AS post_impressions_nonviral
  , CAST(post_impressions_nonviral_unique AS INT64) AS post_impressions_nonviral_unique
  , CAST(post_impressions_organic         AS INT64) AS post_impressions_organic
  , CAST(post_impressions_organic_unique  AS INT64) AS post_impressions_organic_unique
  , CAST(post_impressions_paid            AS INT64) AS post_impressions_paid
  , CAST(post_impressions_paid_unique     AS INT64) AS post_impressions_paid_unique
  , CAST(post_impressions_unique          AS INT64) AS post_impressions_unique
  , CAST(post_impressions_viral           AS INT64) AS post_impressions_viral
  , CAST(post_impressions_viral_unique    AS INT64) AS post_impressions_viral_unique
  , CAST(post_negative_feedback           AS INT64) AS post_negative_feedback
  , CAST(post_negative_feedback_unique    AS INT64) AS post_negative_feedback_unique
  , CAST(post_reactions_anger_total       AS INT64) AS post_reactions_anger_total
  , CAST(post_reactions_haha_total        AS INT64) AS post_reactions_haha_total
  , CAST(post_reactions_like_total        AS INT64) AS post_reactions_like_total
  , CAST(post_reactions_love_total        AS INT64) AS post_reactions_love_total
  , CAST(post_reactions_sorry_total       AS INT64) AS post_reactions_sorry_total
  , CAST(post_reactions_wow_total         AS INT64) AS post_reactions_wow_total

  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'facebook_sipher_post_insights') }}
ORDER BY snapshot_date
)

SELECT * FROM final