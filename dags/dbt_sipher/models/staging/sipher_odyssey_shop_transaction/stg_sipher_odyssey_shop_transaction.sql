{{- config(
    materialized = 'view',
)-}}

WITH stg AS (
    SELECT DISTINCT
        user_id
        , CAST(JSON_EXTRACT_SCALAR(data, '$.CountryCode')       AS STRING)    AS country_code
        , CAST(JSON_EXTRACT_SCALAR(data, '$.CreateTime')        AS INT64)     AS create_time
        , CAST(JSON_EXTRACT_SCALAR(data, '$.CurrencyType')      AS STRING)    AS currency_type
        , CAST(JSON_EXTRACT_SCALAR(data, '$.FiatCurrencyPrice') AS FLOAT64)   AS fiat_currency_price
        , CAST(JSON_EXTRACT_SCALAR(data, '$.FiatType')          AS STRING)    AS fiat_type
        , CAST(JSON_EXTRACT_SCALAR(data, '$.LastUpdated')       AS INT64)     AS last_updated
        , CAST(JSON_EXTRACT_SCALAR(data, '$.OriginalPrice')     AS INT64)     AS original_price
        , CAST(JSON_EXTRACT_SCALAR(data, '$.PendingId')         AS STRING)    AS pending_id
        , CAST(JSON_EXTRACT_SCALAR(data, '$.Price')             AS FLOAT64)   AS price
        , CAST(JSON_EXTRACT_SCALAR(data, '$.Quantity')          AS INT64)     AS quantity
        , CAST(JSON_EXTRACT_SCALAR(data, '$.Result')            AS INT64)     AS result
        , CAST(JSON_EXTRACT_SCALAR(data, '$.RewardId')          AS STRING)    AS reward_id
        , CAST(JSON_EXTRACT_SCALAR(data, '$.ShopItemId')        AS STRING)    AS shop_item_id
        , CAST(JSON_EXTRACT_SCALAR(data, '$.State')             AS STRING)    AS state
        , CAST(JSON_EXTRACT_SCALAR(data, '$.TransactionId')     AS INT64)     AS transaction_id
        , CAST(JSON_EXTRACT_SCALAR(data, '$.CreatorCode')       AS STRING)    AS creator_code
        , updated_at
        , updated_at_date
    FROM 
        {{ source('raw_game_meta', 'sipher_prod_shop_transaction') }}
    WHERE 
        updated_at_date > "2019-01-01"     
)

SELECT * FROM stg
WHERE state = "Success"
