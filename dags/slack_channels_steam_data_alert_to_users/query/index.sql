WITH daily_data AS (
  SELECT
    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS report_date,
    steamId,
    name,
    releaseDate,
    CAST(reviews AS INT64) AS reviews,
    CAST(followers AS INT64) AS followers,
    CAST(reviewScore AS FLOAT64) AS reviewScore,
    CAST(revenue AS FLOAT64) AS revenue,
    CAST(copiesSold AS INT64) AS copiesSold,
    CAST(avgPlaytime AS FLOAT64) AS avgPlaytime
  FROM `sipher-data-platform.raw_steam_games.steam_game_lists_*`
),

data_with_previous AS (
  SELECT
    *,
    LAG(reviews) OVER (PARTITION BY steamId ORDER BY report_date) AS previous_reviews,
    LAG(followers) OVER (PARTITION BY steamId ORDER BY report_date) AS previous_followers,
    LAG(reviewScore) OVER (PARTITION BY steamId ORDER BY report_date) AS previous_reviewScore,
    LAG(revenue) OVER (PARTITION BY steamId ORDER BY report_date) AS previous_revenue,
    LAG(copiesSold) OVER (PARTITION BY steamId ORDER BY report_date) AS previous_copiesSold,
    LAG(avgPlaytime) OVER (PARTITION BY steamId ORDER BY report_date) AS previous_avgPlaytime
  FROM
    daily_data
)

,results AS
(SELECT
  report_date,
  steamId,
  name,
  releaseDate,
  previous_revenue,
  revenue,
  ROUND(revenue - IFNULL(previous_revenue, 0), 2) AS delta_revenue,
  ROUND((revenue - IFNULL(previous_revenue, 0))
    /(CASE WHEN previous_revenue IS NULL OR previous_revenue = 0 THEN 1
    ELSE IFNULL(previous_revenue, 1) END)*100, 2) AS pct_revenue,
  
  previous_copiesSold,
  copiesSold,
  ROUND(copiesSold - IFNULL(previous_copiesSold, 0), 2) AS delta_copiesSold,
  ROUND((copiesSold - IFNULL(previous_copiesSold, 0))
    /(CASE WHEN previous_copiesSold IS NULL OR previous_copiesSold = 0 THEN 1
    ELSE IFNULL(previous_copiesSold, 1) END)*100, 2) AS pct_copiesSold,
  
  previous_avgPlaytime,
  avgPlaytime,
  ROUND(avgPlaytime - IFNULL(previous_avgPlaytime, 0), 2) AS delta_avgPlaytime,
  ROUND((avgPlaytime - IFNULL(previous_avgPlaytime, 0))
    /(CASE WHEN previous_avgPlaytime IS NULL OR previous_avgPlaytime = 0 THEN 1
    ELSE IFNULL(previous_avgPlaytime, 1) END)*100, 2) AS pct_avgPlaytime,

  previous_reviews,
  reviews,
  ROUND(reviews - IFNULL(previous_reviews, 0), 2) AS delta_reviews,
  ROUND((reviews - IFNULL(previous_reviews, 0))
    /(CASE WHEN previous_reviews IS NULL OR previous_reviews = 0 THEN 1
    ELSE IFNULL(previous_reviews, 1) END)*100, 2) AS pct_reviews,

  previous_followers,
  followers,
  ROUND(followers - IFNULL(previous_followers, 0), 2) AS delta_followers,
  ROUND((followers - IFNULL(previous_followers, 0))
    /(CASE WHEN previous_followers IS NULL OR previous_followers = 0 THEN 1
    ELSE IFNULL(previous_followers, 1) END)*100, 2) AS pct_followers,

  previous_reviewScore,
  reviewScore,
  ROUND(reviewScore - IFNULL(previous_reviewScore, 0), 2) AS delta_reviewScore,
  ROUND((reviewScore - IFNULL(previous_reviewScore, 0))
    /(CASE WHEN previous_reviewScore IS NULL OR previous_reviewScore = 0 THEN 1
    ELSE IFNULL(previous_reviewScore, 1) END)*100, 2) AS pct_reviewScore,
  
  
FROM
  data_with_previous
WHERE report_date = CURRENT_DATE()
)

SELECT
  *
FROM 
    results 
WHERE 
    previous_revenue IS NOT NULL
    AND 
    releaseDate > '2023-01-01'
ORDER BY
  pct_revenue DESC