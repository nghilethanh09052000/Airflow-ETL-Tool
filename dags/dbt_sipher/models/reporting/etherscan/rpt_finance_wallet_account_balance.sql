{{- config(
    materialized='table',
    schema='sipher_presentation',
    partition='quote_date'
)-}}

WITH wallet_balance AS(
    SELECT
        *
    FROM
        {{ ref('stg_etherscan_polygon__important_wallet_accounts_balance') }} balance
),

daily_quote AS(
    SELECT
        LOWER(symbol) AS token_symbol,
        close_price,
        quote_date
    FROM
        {{ ref('stg_coinmarketcap__main_token_quotes') }}
),

intraday_quote AS(
    SELECT
        CASE WHEN token_symbol = 'Compound' THEN 'comp' ELSE LOWER(token_symbol) END AS token_symbol,
        price_usd,
        DATE(LEFT(CAST(timestamp AS STRING), 10)) AS quote_date
    FROM
        {{ ref('stg_coinmarketcap__main_token_quotes_intraday') }} QUALIFY ROW_NUMBER() OVER (
            PARTITION BY token_symbol
            ORDER BY
                timestamp DESC
        ) = 1
    ORDER BY
        token_symbol,
        quote_date
)

SELECT
    DISTINCT wallet_address,
    wallet_name,
    token_address,
    wallet_balance.token_symbol AS token_symbol,
    value * POWER(10, - decimal) AS value,
    COALESCE(close_price, price_usd) AS close_price,
    network,
    wallet_balance.quote_date AS quote_date
FROM
    wallet_balance
    LEFT JOIN daily_quote ON daily_quote.quote_date = wallet_balance.quote_date
    AND daily_quote.token_symbol = LOWER(wallet_balance.token_symbol)
    LEFT JOIN intraday_quote ON intraday_quote.quote_date = wallet_balance.quote_date
    AND intraday_quote.token_symbol = LOWER(wallet_balance.token_symbol)