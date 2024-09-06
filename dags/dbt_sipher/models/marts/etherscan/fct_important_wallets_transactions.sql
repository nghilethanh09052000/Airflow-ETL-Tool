{{- config(
    materialized='incremental',
    schema='sipher_presentation',
    partition='date'
)-}}

WITH
ethereum_from_address_transactions AS (
  SELECT
    from_address AS wallet_address,
    CASE
        WHEN LOWER(from_address) = LOWER('0xF5c935c8E6bd741c74f8633a106c0CA33E3c4faf') THEN 'Gnosis Sipher Seed & Partnership Round MSIG'
        WHEN LOWER(from_address) = LOWER('0x11986f428b22c011082820825ca29B21a3C11295') THEN 'Gnosis $SIPHER General Management MSIG'
        WHEN LOWER(from_address) = LOWER('0x1299461a6dc8E755F7299cC221B29776d7eDb663') THEN 'Gnosis NFT OpenSea Commission MSIG'
        WHEN LOWER(from_address) = LOWER('0x94B1a79C1a2a3Fedb40EF3af44DEc1DEd8Bc26f4') THEN 'Gnosis Sipher B2B Guilds MSIG'
        WHEN LOWER(from_address) = LOWER('0x1390047A78a029383d0ADcC1adB5053b8fA3243F') THEN 'Gnosis Sipher NFT Sale MSIG'
        WHEN LOWER(from_address) = LOWER('0x128114f00540a0f59b12DE5e2BaE354FcEdf0aa2') THEN '1102 Equity Fundraising'
        WHEN LOWER(from_address) = LOWER('0x3e8c6676eef25c7b18a7ac24271075f735c79a16') THEN 'Athereal.eth Public Wallet'
        WHEN LOWER(from_address) = LOWER('0x3BC15f3601eA7b65a9A8E7f8C776d4Ab5e2Bc002') THEN 'Sipher Cold Wallet Trezor'
    END AS wallet_name,
    NULL AS token_address,
    'ETH' AS token_symbol,
    18 AS decimal,
    -CAST(value AS NUMERIC) AS value,
    'etherscan' AS network,
    block_timestamp AS timestamp,
    LEFT(CAST(block_timestamp AS STRING), 10) AS date,
    a.hash AS transaction_hash,
  FROM `bigquery-public-data.crypto_ethereum.transactions` a
  WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND LOWER(from_address) IN (
        LOWER("0xF5c935c8E6bd741c74f8633a106c0CA33E3c4faf"),
        LOWER("0x11986f428b22c011082820825ca29B21a3C11295"),
        LOWER("0x1299461a6dc8E755F7299cC221B29776d7eDb663"),
        LOWER("0x94B1a79C1a2a3Fedb40EF3af44DEc1DEd8Bc26f4"),
        LOWER("0x1390047A78a029383d0ADcC1adB5053b8fA3243F"),
        LOWER("0x128114f00540a0f59b12DE5e2BaE354FcEdf0aa2"),
        LOWER("0x3e8c6676eef25c7b18a7ac24271075f735c79a16"),
        LOWER("0x3BC15f3601eA7b65a9A8E7f8C776d4Ab5e2Bc002")
    )
)

,ethereum_to_address_transactions AS(
  SELECT
    to_address AS wallet_address,
    CASE
        WHEN LOWER(to_address) = LOWER('0xF5c935c8E6bd741c74f8633a106c0CA33E3c4faf') THEN 'Gnosis Sipher Seed & Partnership Round MSIG'
        WHEN LOWER(to_address) = LOWER('0x11986f428b22c011082820825ca29B21a3C11295') THEN 'Gnosis $SIPHER General Management MSIG'
        WHEN LOWER(to_address) = LOWER('0x1299461a6dc8E755F7299cC221B29776d7eDb663') THEN 'Gnosis NFT OpenSea Commission MSIG'
        WHEN LOWER(to_address) = LOWER('0x94B1a79C1a2a3Fedb40EF3af44DEc1DEd8Bc26f4') THEN 'Gnosis Sipher B2B Guilds MSIG'
        WHEN LOWER(to_address) = LOWER('0x1390047A78a029383d0ADcC1adB5053b8fA3243F') THEN 'Gnosis Sipher NFT Sale MSIG'
        WHEN LOWER(to_address) = LOWER('0x128114f00540a0f59b12DE5e2BaE354FcEdf0aa2') THEN '1102 Equity Fundraising'
        WHEN LOWER(to_address) = LOWER('0x3e8c6676eef25c7b18a7ac24271075f735c79a16') THEN 'Athereal.eth Public Wallet'
        WHEN LOWER(to_address) = LOWER('0x3BC15f3601eA7b65a9A8E7f8C776d4Ab5e2Bc002') THEN 'Sipher Cold Wallet Trezor'
    END AS wallet_name,
    NULL AS token_address,
    'ETH' AS token_symbol,
    18 AS decimal,
    CAST(value AS NUMERIC) AS value,
    'etherscan' AS network,
    block_timestamp AS timestamp,
    a.hash AS transaction_hash,
    LEFT(CAST(block_timestamp AS STRING), 10) AS date
  FROM `bigquery-public-data.crypto_ethereum.transactions` a
  WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND LOWER(to_address) IN (
        LOWER("0xF5c935c8E6bd741c74f8633a106c0CA33E3c4faf"),
        LOWER("0x11986f428b22c011082820825ca29B21a3C11295"),
        LOWER("0x1299461a6dc8E755F7299cC221B29776d7eDb663"),
        LOWER("0x94B1a79C1a2a3Fedb40EF3af44DEc1DEd8Bc26f4"),
        LOWER("0x1390047A78a029383d0ADcC1adB5053b8fA3243F"),
        LOWER("0x128114f00540a0f59b12DE5e2BaE354FcEdf0aa2"),
        LOWER("0x3e8c6676eef25c7b18a7ac24271075f735c79a16"),
        LOWER("0x3BC15f3601eA7b65a9A8E7f8C776d4Ab5e2Bc002")
    )
)

,polygon_from_address_transactions AS(
    SELECT 
        from_address AS wallet_address,
        CASE
            WHEN LOWER(from_address) = LOWER('0x1299461a6dc8E755F7299cC221B29776d7eDb663') THEN 'Clone Sipher OpenSea Commission MSIG'
            WHEN LOWER(from_address) = LOWER('0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B') THEN 'Dopa JSC Polygon Gnosis SALA'
            WHEN LOWER(from_address) = LOWER('0xb7273C93095F9597FcBFC48837e852F4CF8b39b2') THEN 'Dopa JSC Polygon Gnosis OPEX'
        END AS wallet_name,
        NULL AS token_address,
        "MATIC" AS token_symbol,
        -CAST(value AS NUMERIC) AS value,
        18 AS decimal,
        'polygon' AS network,
        block_timestamp AS timestamp,
        LEFT(CAST(block_timestamp AS STRING), 10) AS date,
        a.hash AS transaction_hash
    FROM `bigquery-public-data.crypto_polygon.transactions` a
    WHERE 
        DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND LOWER(from_address) IN (
            LOWER("0x1299461a6dc8E755F7299cC221B29776d7eDb663"),
            LOWER("0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B"),
            LOWER("0xb7273C93095F9597FcBFC48837e852F4CF8b39b2")
        )
    )

,polygon_to_address_transactions AS(
    SELECT 
        to_address AS wallet_address,
        CASE
            WHEN LOWER(to_address) = LOWER('0x1299461a6dc8E755F7299cC221B29776d7eDb663') THEN 'Clone Sipher OpenSea Commission MSIG'
            WHEN LOWER(to_address) = LOWER('0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B') THEN 'Dopa JSC Polygon Gnosis SALA'
            WHEN LOWER(to_address) = LOWER('0xb7273C93095F9597FcBFC48837e852F4CF8b39b2') THEN 'Dopa JSC Polygon Gnosis OPEX'
        END AS wallet_name,
        NULL AS token_address,
        "MATIC" AS token_symbol,
        CAST(value AS NUMERIC) AS value,
        18 AS decimal,
        'polygon' AS network,
        block_timestamp AS timestamp,
        LEFT(CAST(block_timestamp AS STRING), 10) AS date,
        a.hash AS transaction_hash
    FROM `bigquery-public-data.crypto_polygon.transactions` a
    WHERE 
        DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND 
        LOWER(to_address) IN (
            LOWER("0x1299461a6dc8E755F7299cC221B29776d7eDb663"),
            LOWER("0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B"),
            LOWER("0xb7273C93095F9597FcBFC48837e852F4CF8b39b2")
        )
    )

SELECT * FROM ethereum_from_address_transactions
UNION ALL
SELECT * FROM ethereum_to_address_transactions
UNION ALL
SELECT * FROM polygon_from_address_transactions
UNION ALL
SELECT * FROM polygon_to_address_transactions