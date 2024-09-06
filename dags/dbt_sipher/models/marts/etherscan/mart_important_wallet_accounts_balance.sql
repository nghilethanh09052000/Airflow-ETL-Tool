{{- config(
    materialized='table',
    schema='sipher_presentation',
    partition='date'
)-}}

WITH 
all_data AS (
    SELECT  
        wallet_address,
        wallet_name,
        token_address,
        token_symbol,
        value AS value,
        decimal,
        network,
        PARSE_DATE("%Y-%m-%d", date) AS date,
    FROM  {{ ref('fct_important_wallets_token_transfers')}}
    )

,wallet_date_array AS(
    SELECT DISTINCT
        wallet_address,
        wallet_name,
        network,
        token_address,
        token_symbol,
        decimal,
        date
    FROM all_data, UNNEST(GENERATE_DATE_ARRAY('2021-02-23', CURRENT_DATE())) AS date
    )


,join_date_unnest AS(
    SELECT
        wallet_address,
        wallet_name,
        token_address,
        token_symbol,
        value,
        decimal,
        network,
        date
    FROM all_data
    RIGHT JOIN wallet_date_array USING(wallet_address, wallet_name, network, token_address, token_symbol, decimal, date)
    ORDER BY wallet_address, date, network, token_symbol
    )

,token_quote AS(
    SELECT DISTINCT
        symbol,
        quote_date,
        open_price,
        close_price
    FROM {{ ref('stg_coinmarketcap__main_token_quotes')}}
    )

,token_quote_intraday_latest AS(
    SELECT
        token_symbol, 
        price_usd,
        DATE(LEFT(CAST(timestamp AS STRING),10)) AS quote_date
    FROM {{ ref('stg_coinmarketcap__main_token_quotes_intraday')}}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY token_symbol ORDER BY timestamp DESC) = 1
    ORDER BY 1, 3
)

SELECT
  wallet_address,
  wallet_name,
  token_address,
  join_date_unnest.token_symbol AS token_symbol,
  SUM(value) OVER (
            PARTITION BY wallet_address, wallet_name, token_address, join_date_unnest.token_symbol, network
            ORDER BY date
            ROWS UNBOUNDED PRECEDING)*POWER(10,-decimal) AS value,
  decimal,
  open_price,
  COALESCE(close_price, price_usd) AS close_price,
  network,
  NULL AS timestamp,
  date
FROM join_date_unnest
LEFT JOIN token_quote ON token_quote.quote_date = join_date_unnest.date AND LOWER(token_quote.symbol) = LOWER(join_date_unnest.token_symbol)
LEFT JOIN token_quote_intraday_latest ON token_quote_intraday_latest.quote_date = join_date_unnest.date AND LOWER(token_quote_intraday_latest.token_symbol) = LOWER(join_date_unnest.token_symbol)