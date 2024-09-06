{{- config(
  materialized='table',
  partition_by={
    'field': 'updated_at_date',
    'data_type': 'DATE',
},
) -}}

WITH pre_fct AS (
    SELECT  
    s.user_id
    , i.day0_date_tzutc
    , CASE
        WHEN country_code IS NULL THEN "WEB"
        ELSE country_code
    END AS country_code
    , create_time
    , EXTRACT(DATE FROM TIMESTAMP_SECONDS(CAST((s.create_time/1000) AS INT64)))  AS create_date
    , currency_type
    , fiat_currency_price
    , CASE
        WHEN fiat_type = "" AND country_code IS NULL AND currency_type = "Fiat" THEN "Xsolla"
        WHEN fiat_type = "" AND currency_type != "Fiat"                         THEN "in game"
        ELSE fiat_type
    END AS fiat_type
    , last_updated
    , EXTRACT(DATE FROM TIMESTAMP_SECONDS(CAST((s.last_updated/1000) AS INT64)))  AS last_updated_date
    , original_price
    , pending_id         AS order_id
    , price
    , quantity
    , (price * quantity) AS amount
    , result
    , reward_id
    , shop_item_id
    , state
    , transaction_id
    , creator_code
    , updated_at
    , updated_at_date
    FROM 
      {{ ref('stg_sipher_odyssey_shop_transaction')}} s
    LEFT JOIN 
      (SELECT 
        DISTINCT user_id, 
        MIN(day0_date_tzutc) AS day0_date_tzutc 
      FROM {{ ref('int_sipher_odyssey_player_day0_version')}}
        GROUP BY user_id) i
    ON s.user_id = i.user_id
    
)
, creator AS (
  SELECT 
    user_id
    , creator_code
    , MIN(updated_at) AS min_updated_at
  FROM pre_fct 
  WHERE creator_code IS NOT NULL
  GROUP BY 1,2
)
, fct AS (
  SELECT
    pf.user_id
    , day0_date_tzutc
    , country_code
    , create_time
    , create_date
    , currency_type
    , fiat_currency_price
    , fiat_type
    , last_updated
    , last_updated_date
    , original_price
    , order_id
    , price
    , quantity
    , amount
    , result
    , reward_id
    , shop_item_id
    , state
    , transaction_id
    , CASE 
        WHEN pf.creator_code IS NULL THEN c.creator_code
        ELSE pf.creator_code
    END AS creator_code
    , updated_at
    , updated_at_date
    , DATE_DIFF(create_date, day0_date_tzutc, day) AS day_diff
  FROM pre_fct pf
  LEFT JOIN 
    creator c
  ON  
    pf.user_id = c.user_id
)
, refund AS (
  SELECT
      Order_Number
      , Financial_Status
  FROM 
      {{ source('raw_google_iap', 'raw_Atherlabs_financial_reports__estimated_sales_reports') }}
  WHERE 
      Financial_Status = "Refund"
)
, internal_tester AS(
  SELECT 
    order_id
    , "internal_tester" AS internal_check
  FROM {{ ref('int_sipher_odyssey_transactions_sipher_google')}}
  WHERE missing_transaction_google = "missing"
)
, not_refund AS (
  SELECT
    fct.*
    , r.Financial_Status AS financial_status
    , it.internal_check AS internal_check
  FROM fct 
  LEFT JOIN refund r
  ON fct.order_id = r.Order_Number
  LEFT JOIN internal_tester it
  ON fct.order_id = it.order_id
)

SELECT * FROM not_refund 
