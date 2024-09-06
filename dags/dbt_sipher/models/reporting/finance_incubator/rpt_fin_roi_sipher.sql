{{- config(
    materialized = 'table',
    partition_by={
		'field': 'date',
		'data_type': 'DATE',
  }
)-}}

WITH rev AS (
  SELECT  
    date
    , SUM(gross_revenue) AS gross_revenue
    , SUM(cost) AS cost_ads
  FROM {{ ref('rpt_fin_incubator_roas')}}
  GROUP BY ALL
)
, c_gcp AS (
  SELECT 
    start_time
    , ROUND(SUM(cost), 3) AS cost_gcp 
  FROM 
    {{ source('gcp_billing', 'cost_analysis')}}
  WHERE billing_account_name = 'Atherlabs'
  GROUP BY 1
)
, c_aws AS (
  SELECT 
    bill_act_date
    , ROUND(SUM(unblended_cost), 3) AS cost_aws 
  FROM 
    {{ source('sipher_presentation', 'aws_billing')}}
  WHERE aws_account = 'game_production'
  GROUP BY 1
)

SELECT
  r.date
  , gross_revenue
  , cost_ads
  , cost_gcp  AS cost_gcp
  , cost_aws  AS cost_aws
FROM rev r
LEFT JOIN c_gcp g
ON r.date = g.start_time
LEFT JOIN c_aws a
ON r.date = a.bill_act_date