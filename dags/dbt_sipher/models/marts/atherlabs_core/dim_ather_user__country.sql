{{- config(
    materialized = 'table'
)-}}

WITH a AS (
    SELECT 
        DISTINCT user_id
    FROM {{ ref('dim_ather_user__all')}}
)
, b AS (
  SELECT 
    PARSE_DATE('%Y%m%d', event_date) AS first_event_date
    , event_timestamp                AS first_event_timestamp
    , user_id
    , geo.city
    , geo.country
    , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY 1 ASC, 2 ASC) AS rn
  FROM `sipher-atherlabs-ga.analytics_341181237.events_*` 
  WHERE user_id IS NOT NULL
)
, c AS (
  SELECT 
    a.*
    , ba.first_event_date
    , ba.city
    , ba.country
  FROM a
  LEFT JOIN (SELECT * FROM b WHERE rn = 1) ba
  ON a.user_id = ba.user_id
)
--- get distinct ather_id, country from game data
, e AS (
    SELECT  
        DISTINCT ather_id
        , country
        , ROW_NUMBER() OVER (PARTITION BY ather_id ORDER BY current_build_timestamp ASC) AS rn
    FROM {{ ref('dim_sipher_odyssey_player') }}
    WHERE ather_id IS NOT NULL
)
--- get ather_id (user_id), first_create_date 
, g AS (
    SELECT 
        DISTINCT user_id
        , MIN(user_create_date) AS user_create_date 
    FROM {{ ref('stg_aws__ather_id__raw_cognito') }}
    GROUP BY 1
)
, g1 AS (
    SELECT 
        DISTINCT user_id
        , MIN(created_at) AS created_at 
    FROM {{ ref('stg_aws__ather_id__raw_user') }}
    GROUP BY 1
)
--- join w game data 
, f AS (
    SELECT
        DISTINCT c.user_id
        , COALESCE(c.first_event_date, PARSE_DATE('%Y-%m-%d', FORMAT_TIMESTAMP('%Y-%m-%d', g.user_create_date)), PARSE_DATE('%Y-%m-%d', FORMAT_TIMESTAMP('%Y-%m-%d', g1.created_at))) AS first_event_date
        , c.city
        , COALESCE(c.country, e.country) as country
    FROM c
    LEFT JOIN (SELECT * FROM e WHERE rn =1) e ON c.user_id = e.ather_id
    LEFT JOIN g  ON c.user_id = g.user_id
    LEFT JOIN g1 ON c.user_id = g1.user_id
)

SELECT * FROM f

