{{- config(
  materialized='table',
  partition_by={
    'field': 'updated_at_date',
    'data_type': 'DATE',
},
) -}}

WITH stg AS (  
    SELECT    
        user_id    
        , country_code    
        , fiat_currency_price       
        , pending_id    
        , reward_id    
        , shop_item_id    
        , state    
        , transaction_id    
        , updated_at_date    
    FROM 
        {{ ref('stg_sipher_odyssey_shop_transaction') }}    
    WHERE  
        pending_id LIKE "GPA%" 
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
, pre_google AS (
    SELECT  
        DISTINCT rp.Order_Number AS order_id
        , CASE 
            WHEN rf.Financial_Status IS NULL THEN rp.Financial_Status
            ELSE rf.Financial_Status
        END AS status
        , Product_Title                        AS google_product_item
        , Currency_of_Sale                     AS google_item_currency
        , Item_Price                           AS google_item_price
        , MIN (Order_Charged_Date)             AS google_updated_date
    FROM 
        {{ source('raw_google_iap', 'raw_Atherlabs_financial_reports__estimated_sales_reports') }} rp
    LEFT JOIN refund rf
    ON rp.Order_Number = rf.Order_Number
    GROUP BY 1,2,3,4,5
)
, checking AS (
    SELECT 
        COALESCE(CAST(google_updated_date AS DATE), stg.updated_at_date) AS updated_at_date
        , stg.user_id
        , COALESCE(p.order_id, stg.pending_id)                           AS order_id
        , CASE       
            WHEN p.order_id IS NULL THEN "missing"
            ELSE NULL
        END missing_transaction_google
        , CASE
            WHEN stg.pending_id IS NULL THEN "missing" 
            ELSE NULL  
        END missing_transaction_game
        , status
        , google_item_price
        , google_item_currency
        , stg.fiat_currency_price AS sipher_fiat_price_usd
        , google_product_item
        , stg.shop_item_id        AS sipher_shop_item
        , "Sipher - Google"       AS platfrom
    FROM pre_google p
    FULL JOIN stg 
    ON p.order_id = stg.pending_id
)
, currency AS (
  SELECT
    sipher_fiat_price_usd
    , google_product_item
    , sipher_shop_item
  FROM checking
  WHERE 
    google_item_price IS NOT NULL AND google_item_currency IS NOT NULL AND sipher_fiat_price_usd IS NOT NULL AND google_item_currency != "USD"
  GROUP BY 1,2,3
)
, final AS (
SELECT  
    updated_at_date
    , user_id
    , order_id
    , missing_transaction_google
    , missing_transaction_game
    , status
    , google_item_price
    , google_item_currency
    , CASE 
        WHEN ch.sipher_fiat_price_usd IS NULL THEN c.sipher_fiat_price_usd
        ELSE ch.sipher_fiat_price_usd
    END AS sipher_fiat_price_usd
    , ch.google_product_item
    , CASE
        WHEN ch.sipher_shop_item IS NULL THEN c.sipher_shop_item
        ELSE ch.sipher_shop_item
    END AS sipher_shop_item
    , platfrom
  FROM checking ch
  LEFT JOIN currency c
  ON ch.google_product_item = c.google_product_item
)

SELECT * FROM final