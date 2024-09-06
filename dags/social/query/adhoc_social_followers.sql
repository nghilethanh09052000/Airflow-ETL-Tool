CREATE OR REPLACE TABLE `sipher-data-platform.reporting_social.adhoc_social_followers` AS 

SELECT
  (MAX(CAST(tiktok.follower_count AS INT64)) + MAX(facebook.followers_count) + MAX(twitter.followers_cnt) + MAX(discord.followers_count) + MAX(youtube.subscriber_count)) AS total_followers
FROM `sipher-data-platform.raw_social.tiktok_profile` tiktok
CROSS JOIN `sipher-data-platform.staging_social.stg_facebook_page_overall` facebook 
CROSS JOIN `sipher-data-platform.staging_social.stg_twitter_profile_stats` twitter
CROSS JOIN `sipher-data-platform.staging_social.stg_discord_adhoc_page_followers` discord
CROSS JOIN `sipher-data-platform.staging_social.stg_youtube_overview` youtube
WHERE tiktok.snapshot_date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND CURRENT_DATE()
AND twitter.snapshot_date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND CURRENT_DATE()
AND facebook.snapshot_date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND CURRENT_DATE()
AND youtube.snapshot_date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND CURRENT_DATE()
AND PARSE_DATE("%Y/%m/%d", discord.date) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY) AND CURRENT_DATE()
