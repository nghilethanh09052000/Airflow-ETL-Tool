{{- config(
    materialized='incremental',
    schema='sipher_presentation',
    partition='date'
)-}}

WITH
etherscan_data_from_address AS(
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
        token_address,
        CASE
            WHEN LOWER(token_address) = LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7') THEN 'USDT'
            WHEN LOWER(token_address) = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') THEN 'USDC'
            WHEN LOWER(token_address) = LOWER('0x39AA39c021dfbaE8faC545936693aC917d5E7563') THEN 'cUSDC'
            WHEN LOWER(token_address) = LOWER('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9') THEN 'cUSDT'
            WHEN LOWER(token_address) = LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599') THEN 'wBTC'
            WHEN LOWER(token_address) = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN 'stETH'
            WHEN LOWER(token_address) = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') THEN 'WETH'
            WHEN LOWER(token_address) = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') THEN 'wstETH'
            WHEN LOWER(token_address) = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN 'COMP'
        END AS token_symbol,
        -CAST(value AS NUMERIC) AS value,
        CASE
            WHEN LOWER(token_address) = LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7') THEN 6
            WHEN LOWER(token_address) = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') THEN 6
            WHEN LOWER(token_address) = LOWER('0x39AA39c021dfbaE8faC545936693aC917d5E7563') THEN 8
            WHEN LOWER(token_address) = LOWER('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9') THEN 8
            WHEN LOWER(token_address) = LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599') THEN 8
            WHEN LOWER(token_address) = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN 18
            WHEN LOWER(token_address) = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') THEN 18
            WHEN LOWER(token_address) = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') THEN 18
            WHEN LOWER(token_address) = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN 18
        END AS decimal,
        'etherscan' AS network,
        block_timestamp AS timestamp,
        LEFT(CAST(block_timestamp AS STRING), 10) AS date,
        transaction_hash
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE 
        DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND LOWER(token_address) IN (
            LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7'),
            LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
            LOWER('0x39AA39c021dfbaE8faC545936693aC917d5E7563'),
            LOWER('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9'),
            LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'),
            LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'),
            LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),
            LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'),
            LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888')
        )
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

,etherscan_data_to_address AS(
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
        token_address,
        CASE
            WHEN LOWER(token_address) = LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7') THEN 'USDT'
            WHEN LOWER(token_address) = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') THEN 'USDC'
            WHEN LOWER(token_address) = LOWER('0x39AA39c021dfbaE8faC545936693aC917d5E7563') THEN 'cUSDC'
            WHEN LOWER(token_address) = LOWER('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9') THEN 'cUSDT'
            WHEN LOWER(token_address) = LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599') THEN 'wBTC'
            WHEN LOWER(token_address) = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN 'stETH'
            WHEN LOWER(token_address) = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') THEN 'WETH'
            WHEN LOWER(token_address) = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') THEN 'wstETH'
            WHEN LOWER(token_address) = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN 'COMP'
        END AS token_symbol,
        CAST(value AS NUMERIC) AS value,
        CASE
            WHEN LOWER(token_address) = LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7') THEN 6
            WHEN LOWER(token_address) = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') THEN 6
            WHEN LOWER(token_address) = LOWER('0x39AA39c021dfbaE8faC545936693aC917d5E7563') THEN 8
            WHEN LOWER(token_address) = LOWER('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9') THEN 8
            WHEN LOWER(token_address) = LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599') THEN 8
            WHEN LOWER(token_address) = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN 18
            WHEN LOWER(token_address) = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') THEN 18
            WHEN LOWER(token_address) = LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') THEN 18
            WHEN LOWER(token_address) = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN 18
        END AS decimal,
        'etherscan' AS network,
        block_timestamp AS timestamp,
        LEFT(CAST(block_timestamp AS STRING), 10) AS date,
        transaction_hash
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE 
        DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND LOWER(token_address) IN (
            LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7'),
            LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
            LOWER('0x39AA39c021dfbaE8faC545936693aC917d5E7563'),
            LOWER('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9'),
            LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'),
            LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'),
            LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),
            LOWER('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'),
            LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888')
        )
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


,polygon_data_from_address AS(
    SELECT 
        from_address AS wallet_address,
        CASE
            WHEN LOWER(from_address) = LOWER('0x1299461a6dc8E755F7299cC221B29776d7eDb663') THEN 'Clone Sipher OpenSea Commission MSIG'
            WHEN LOWER(from_address) = LOWER('0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B') THEN 'Dopa JSC Polygon Gnosis SALA'
            WHEN LOWER(from_address) = LOWER('0xb7273C93095F9597FcBFC48837e852F4CF8b39b2') THEN 'Dopa JSC Polygon Gnosis OPEX'
        END AS wallet_name,
        token_address,
        CASE
            WHEN LOWER(token_address) = LOWER('0xc2132D05D31c914a87C6611C10748AEb04B58e8F') THEN 'USDT'
            WHEN LOWER(token_address) = LOWER('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174') THEN 'USDC'
            WHEN LOWER(token_address) = LOWER('0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6') THEN 'wBTC'
            WHEN LOWER(token_address) = LOWER('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619') THEN 'WETH'
            WHEN LOWER(token_address) = LOWER('0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c') THEN 'COMP'
        END AS token_symbol,
        -CAST(value AS NUMERIC) AS value,
        CASE
            WHEN LOWER(token_address) = LOWER('0xc2132D05D31c914a87C6611C10748AEb04B58e8F') THEN 6
            WHEN LOWER(token_address) = LOWER('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174') THEN 6
            WHEN LOWER(token_address) = LOWER('0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6') THEN 8
            WHEN LOWER(token_address) = LOWER('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619') THEN 18
            WHEN LOWER(token_address) = LOWER('0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c') THEN 18
        END AS decimal,
        'polygon' AS network,
        block_timestamp AS timestamp,
        LEFT(CAST(block_timestamp AS STRING), 10) AS date,
        transaction_hash
    FROM `bigquery-public-data.crypto_polygon.token_transfers` 
    WHERE 
        DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND LOWER(token_address) IN (
            LOWER('0xc2132D05D31c914a87C6611C10748AEb04B58e8F'),
            LOWER('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'),
            LOWER('0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6'),
            LOWER('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619'),
            LOWER('0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c')
        )
        AND LOWER(from_address) IN (
            LOWER("0x1299461a6dc8E755F7299cC221B29776d7eDb663"),
            LOWER("0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B"),
            LOWER("0xb7273C93095F9597FcBFC48837e852F4CF8b39b2")
        )
    )

,polygon_data_to_address AS(
    SELECT 
        to_address AS wallet_address,
        CASE
            WHEN LOWER(to_address) = LOWER('0x1299461a6dc8E755F7299cC221B29776d7eDb663') THEN 'Clone Sipher OpenSea Commission MSIG'
            WHEN LOWER(to_address) = LOWER('0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B') THEN 'Dopa JSC Polygon Gnosis SALA'
            WHEN LOWER(to_address) = LOWER('0xb7273C93095F9597FcBFC48837e852F4CF8b39b2') THEN 'Dopa JSC Polygon Gnosis OPEX'
        END AS wallet_name,
        token_address,
        CASE
            WHEN LOWER(token_address) = LOWER('0xc2132D05D31c914a87C6611C10748AEb04B58e8F') THEN 'USDT'
            WHEN LOWER(token_address) = LOWER('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174') THEN 'USDC'
            WHEN LOWER(token_address) = LOWER('0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6') THEN 'wBTC'
            WHEN LOWER(token_address) = LOWER('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619') THEN 'WETH'
            WHEN LOWER(token_address) = LOWER('0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c') THEN 'COMP'
        END AS token_symbol,
        CAST(value AS NUMERIC) AS value,
        CASE
            WHEN LOWER(token_address) = LOWER('0xc2132D05D31c914a87C6611C10748AEb04B58e8F') THEN 6
            WHEN LOWER(token_address) = LOWER('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174') THEN 6
            WHEN LOWER(token_address) = LOWER('0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6') THEN 8
            WHEN LOWER(token_address) = LOWER('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619') THEN 18
            WHEN LOWER(token_address) = LOWER('0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c') THEN 18
        END AS decimal,
        'polygon' AS network,
        block_timestamp AS timestamp,
        LEFT(CAST(block_timestamp AS STRING), 10) AS date,
        transaction_hash
    FROM `bigquery-public-data.crypto_polygon.token_transfers` 
    WHERE 
        DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND LOWER(token_address) IN (
            LOWER('0xc2132D05D31c914a87C6611C10748AEb04B58e8F'),
            LOWER('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'),
            LOWER('0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6'),
            LOWER('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619'),
            LOWER('0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c')
        )
        AND 
        LOWER(to_address) IN (
            LOWER("0x1299461a6dc8E755F7299cC221B29776d7eDb663"),
            LOWER("0xf5D57a3EC1fB77f12D1FB9bf761a93A3ddD7c35B"),
            LOWER("0xb7273C93095F9597FcBFC48837e852F4CF8b39b2")
        )
    )

SELECT * FROM etherscan_data_from_address
UNION ALL
SELECT * FROM etherscan_data_to_address
UNION ALL
SELECT * FROM polygon_data_from_address
UNION ALL
SELECT * FROM polygon_data_to_address