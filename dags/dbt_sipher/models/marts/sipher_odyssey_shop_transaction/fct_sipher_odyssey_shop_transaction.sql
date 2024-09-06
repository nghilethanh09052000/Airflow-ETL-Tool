{{- config(
  materialized='table',
  partition_by={
    'field': 'updated_at_date',
    'data_type': 'DATE',
},
) -}}

WITH pre AS (
  SELECT  
    updated_at_date
    , user_id
    , order_id
    , missing_transaction_google
    , missing_transaction_game
    , status
    , google_item_price
    , google_item_currency
    , sipher_fiat_price_usd
    , google_product_item
    , sipher_shop_item
    , platfrom
  FROM 
    {{ ref('int_sipher_odyssey_transactions_sipher_google')}}
  WHERE missing_transaction_game = "missing" AND status = "Charged"
)
, miss_google AS (
  SELECT  
    user_id
    , CAST(NULL AS DATE) AS day0_date_tzutc
    , CAST(NULL AS STRING) AS country_code
    , CAST(NULL AS INT64) AS create_time
    , updated_at_date AS create_date
    , "Fiat" AS currency_type
    , sipher_fiat_price_usd AS fiat_currency_price
    , "Google" AS fiat_type
    , CAST(NULL AS INT64) AS last_updated
    , updated_at_date AS last_updated_date
    , sipher_fiat_price_usd AS original_price
    , order_id
    , sipher_fiat_price_usd AS price
    , CAST(NULL AS INT64) AS quantity
    , CAST(NULL AS FLOAT64) AS amount
    , 0 AS result
    , sipher_shop_item AS reward_id
    , sipher_shop_item AS shop_item_id
    , CASE
        WHEN status = "Charged" THEN "Success" 
        ELSE status
      END AS state
    , CAST(NULL AS INT64) AS transaction_id
    , CAST(NULL AS STRING) AS creator_code
    , CAST(NULL AS STRING) AS updated_at
    , updated_at_date
    , CAST(NULL AS INT64)AS day_diff
  FROM pre
)

SELECT * FROM miss_google

UNION ALL 

SELECT * EXCEPT (financial_status, internal_check) 
FROM {{ ref('int_sipher_odyssey_shop_transaction') }}
WHERE financial_status IS NULL AND internal_check IS NULL
