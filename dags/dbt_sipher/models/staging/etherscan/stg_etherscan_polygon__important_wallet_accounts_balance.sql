{{- config(
    materialized = 'view',
)-}}

SELECT
    wallet_address,
    wallet_name,
    token_address,
    token_symbol,
    SAFE_CAST(value AS NUMERIC) AS value,
    decimal,
    network,
    timestamp,
    DATE(LEFT(CAST(date AS STRING), 10)) AS quote_date
FROM {{ source('raw_etherscan', 'etherscan_polygon_important_wallet_accounts_balance') }}