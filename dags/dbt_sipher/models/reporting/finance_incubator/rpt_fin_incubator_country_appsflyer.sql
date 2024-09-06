{{- config(
    materialized = 'table',
    partition_by={
    'field': 'date',
    'data_type': 'DATE',
  }
)-}}

WITH a AS (
SELECT  
    PARSE_DATE('%Y-%m-%d', dt)                             AS date_tzutc
    , UPPER(platform) AS os
    , country_code
    , SUM(COALESCE(CAST(event_revenue_usd AS FLOAT64), 0)) AS revenue
FROM {{ source('raw_game_appsflyer_locker', 'inapps') }}
GROUP BY all
)

SELECT
    r.date
    , COALESCE(r.country_code, a.country_code) AS country_code
    , r.country
    , COALESCE(r.os, a.os) AS os
    , a.revenue AS rev_appsflyers
    , r.gross_revenue AS rev_shop
    , r.installs
FROM {{ ref('rpt_fin_incubator_roas') }} r
LEFT JOIN a
ON r.date = a.date_tzutc AND r.os = a.os AND r.country_code = a.country_code