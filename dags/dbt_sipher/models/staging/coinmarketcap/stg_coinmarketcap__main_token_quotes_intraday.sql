{{- config(
    materialized = 'view',
)-}}

SELECT
    token_symbol,			
    price_usd,			
    market_cap_usd,
    vol_24h,
    price_btc,			
    market_cap_btc,
    TIMESTAMP_SECONDS(CAST(timestamp AS INT64)) AS	timestamp
FROM {{ source('raw_coinmarketcap', 'main_token_quotes_intraday') }}