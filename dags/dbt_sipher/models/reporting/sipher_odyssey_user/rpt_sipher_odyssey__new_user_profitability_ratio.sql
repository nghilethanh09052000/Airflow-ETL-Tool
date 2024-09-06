{{- config(
  materialized='table',
  partition_by={
    "field": "day0_date_tzutc",
    "data_type": "date",
},
) -}}

WITH day0_user AS (
  SELECT 
  day0_date_tzutc
  , day0_os
  , COUNT(DISTINCT user_id) AS new_users  
  FROM 
    `sipher-data-platform.reporting_sipher_odyssey__game_product.fact_game_product_health__dau`
  GROUP BY 1,2
)
,pre_revenue AS (
  SELECT 
    pre.user_id
    , CASE 
        WHEN da.day0_os IS NULL AND fiat_type = 'Apple' OR fiat_type = 'Xsolla' OR fiat_type = 'Crypto' THEN 'IOS'
        WHEN da.day0_os IS NULL AND fiat_type = 'Google' THEN 'ANDROID'
        ELSE UPPER(da.day0_os)
    END AS day0_os
    , CASE 
        WHEN pre.day0_date_tzutc IS NULL THEN (MIN(pre.updated_at_date) OVER (PARTITION BY pre.user_id))
        ELSE pre.day0_date_tzutc
    END AS day0_date_tzutc
    , pre.fiat_type 
    , pre.fiat_currency_price
    , pre.updated_at_date
  FROM 
    {{ ref('fct_sipher_odyssey_shop_transaction')}} pre
  LEFT JOIN (SELECT user_id, day0_os FROM `sipher-data-platform.reporting_sipher_odyssey__game_product.fact_game_product_health__dau` GROUP BY 1,2) da
  ON pre.user_id = da.user_id
  WHERE fiat_type != "in game"
)
, re AS (
  SELECT
    day0_date_tzutc
    , day0_os
    , fiat_type
    , ROUND(sum(fiat_currency_price), 3) AS revenue
  FROM pre_revenue
  GROUP BY 1,2,3
)
, r1 AS (
  SELECT * FROM
    (SELECT day0_date_tzutc, day0_os, fiat_type, revenue FROM re WHERE day0_os = 'ANDROID') 
    PIVOT (sum(revenue) FOR fiat_type IN ('Apple', 'Google', 'Xsolla', 'Crypto')) 
)
, r2 AS (
  SELECT * FROM
    (SELECT day0_date_tzutc, day0_os, fiat_type, revenue FROM re WHERE day0_os = 'IOS') 
    PIVOT (sum(revenue) FOR fiat_type IN ('Apple', 'Google', 'Xsolla', 'Crypto')) 
)
, rev AS (
  SELECT * FROM r1
  UNION ALL
  SELECT * FROM r2
)
, c_gcp AS (
  SELECT 
    start_time
    , ROUND(SUM(cost), 3) AS cost_gcp 
  FROM 
    `sipher-data-platform.gcp_billing.cost_analysis`
  GROUP BY 1
)
, c_aws AS (
  SELECT 
    bill_act_date
    , ROUND(SUM(unblended_cost), 2) AS cost_aws 
  FROM 
    `sipher-data-platform.sipher_presentation.aws_billing` 
  GROUP BY 1
)
, te AS (
  SELECT
    d.day0_date_tzutc
    , UPPER(d.day0_os) AS day0_os
    , d.new_users
    , ROUND((CAST(COALESCE(g.cost_gcp,0) AS FLOAT64) / SUM(new_users) OVER(PARTITION BY day0_date_tzutc)) * d.new_users, 3) AS cost_gcp
    , ROUND((CAST(COALESCE(a.cost_aws,0) AS FLOAT64) / SUM(new_users) OVER(PARTITION BY day0_date_tzutc)) * d.new_users, 3) AS cost_aws
  FROM day0_user d
  LEFT JOIN c_gcp g
  ON d.day0_date_tzutc = g.start_time
  LEFT JOIN c_aws a
  ON d.day0_date_tzutc = a.bill_act_date
)

, f AS (
  SELECT 
    te.day0_date_tzutc
    , te.day0_os
    , te.new_users
    , CAST(COALESCE(rev.Apple,0)      AS FLOAT64) AS Apple
    , CAST(COALESCE(rev.Google,0)     AS FLOAT64) AS Google
    , CAST(COALESCE(rev.Xsolla,0)     AS FLOAT64) AS Xsolla
    , CAST(COALESCE(rev.Crypto,0)     AS FLOAT64) AS Crypto
    , te.cost_gcp
    , te.cost_aws
  FROM te
  LEFT JOIN rev 
  ON te.day0_date_tzutc = rev.day0_date_tzutc AND te.day0_os = rev.day0_os
)
, fin AS (
  SELECT
    day0_date_tzutc
    , day0_os
    , new_users
    , ROUND((Apple + Google + Xsolla + Crypto)/new_users,3) AS rev_p_user_os
    , ROUND((cost_gcp + cost_aws)/new_users,3)              AS cost_p_user
    , Apple
    , Google
    , Xsolla
    , Crypto
    , ROUND((Apple + Google + Xsolla + Crypto),3)           AS total_revenue
    , cost_gcp
    , cost_aws
    , ROUND((cost_gcp + cost_aws),3)                        AS total_cost
  FROM f
)
, final AS (
  SELECT
    day0_date_tzutc
    , day0_os
    , new_users
    , rev_p_user_os
    , ROUND(((SUM(total_revenue) OVER (PARTITION BY day0_date_tzutc)) / SUM(new_users) OVER (PARTITION BY day0_date_tzutc)), 3) AS rev_p_user
    , cost_p_user
    , Apple
    , Google
    , Xsolla
    , Crypto
    , total_revenue
    , cost_gcp
    , cost_aws
    , total_cost
  FROM fin
)

SELECT * FROM final

