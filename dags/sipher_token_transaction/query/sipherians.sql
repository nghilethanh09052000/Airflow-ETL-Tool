CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_ethereum.sipherians` AS

WITH 
    nftdata AS
    (
        SELECT 
            'erc721' AS token_type,
            1 AS is_nft,
            * EXCEPT (token_type)
        FROM `sipher-data-platform.raw_etherscan.etherscan_nft_erc721`
        UNION ALL
        SELECT 
            'erc1155' AS token_type,
            1 AS is_nft,
            * EXCEPT (token_type)
        FROM `sipher-data-platform.raw_etherscan.etherscan_nft_erc1155`
    )
    ,nft_not_in AS 
    (
        SELECT 
            DISTINCT 
            token_contract AS address,
            url_symbol,
            token_name,
            token_type,
            symbol,
            CAST(decimals AS INT64) AS decimals,
            offical_site,
            1 AS is_NFT
        FROM nftdata WHERE LOWER(token_contract) NOT IN (SELECT LOWER(address) FROM `bigquery-public-data.crypto_ethereum.tokens` )
    )
    ,tokens_mapping AS 
    (
        SELECT 
            DISTINCT 
            nftdata.token_contract AS address,
            nftdata.url_symbol,
            nftdata.token_name,
            nftdata.token_type,
            nftdata.symbol,
            CAST(nftdata.decimals AS INT64) AS decimals,
            nftdata.offical_site,
            1 AS is_NFT
        FROM `bigquery-public-data.crypto_ethereum.tokens`  AS t1
        LEFT JOIN nftdata ON LOWER(t1.address) = LOWER(nftdata.token_contract)
        WHERE LOWER(address) IN (SELECT LOWER(token_contract) FROM nftdata)

        UNION ALL 

        SELECT
            DISTINCT 
            address,
            '' AS url_symbol,
            name AS token_name,
            'token' AS token_type,
            symbol,
            CAST(decimals AS INT64) AS decimals,
            '' AS offical_site,
            0 AS is_NFT
        FROM `bigquery-public-data.crypto_ethereum.tokens` 
        WHERE LOWER(address) NOT IN (SELECT LOWER(token_contract) FROM nftdata)
    )   
    , all_tokens AS 
    (
        SELECT * FROM nft_not_in
        WHERE address IS NOT NULL
        UNION ALL 
        SELECT * FROM tokens_mapping
        WHERE address IS NOT NULL
    )
    ,transactions AS
    (
        SELECT 
            COALESCE(all_tk.token_name, t.token_name) AS token_name,
            COALESCE(all_tk.symbol, t.symbol) AS symbol,
            -- COALESCE(all_tk.decimals, t.decimals) AS decimals,
            t.* EXCEPT (token_name, symbol, decimals),
            all_tk.* EXCEPT(token_name, symbol),
            CASE
                WHEN LOWER(t.token_address) = LOWER(all_tk.address) THEN 1
                ELSE 0
            END AS is_mapping
        FROM `sipher-data-platform.sipher_ethereum.token_transaction_sipherians` AS t
        LEFT JOIN all_tokens AS all_tk
        ON LOWER(t.token_address) = LOWER(all_tk.address)
    )
    ,data_token AS
    (
        SELECT 
            from_address AS user_address,
            * EXCEPT (token_type),
            CASE 
                WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN 'token'
                WHEN LOWER(token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN 'erc721'
                WHEN LOWER(token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN 'erc721'
                ELSE token_type
            END AS token_type,
            CASE 
                WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN -SAFE_CAST(value AS NUMERIC)*POWER(1, -18)
                WHEN LOWER(token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN CAST(-1 AS NUMERIC )
                WHEN LOWER(token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN CAST(-1 AS NUMERIC )
                WHEN is_NFT = 1 THEN CAST(-1 AS NUMERIC )
                WHEN decimals > 0 THEN -SAFE_CAST(value AS NUMERIC)*POWER(1, -decimals)
                ELSE -SAFE_CAST(value AS NUMERIC  )
            END AS value_total
        FROM transactions  
        WHERE is_mapping = 1 OR LOWER(token_address) IN 
            (LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511'),
            LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7'),
            LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982')
            )

        UNION ALL 
        
        SELECT 
            to_address AS user_address,
            * EXCEPT (token_type),
            CASE 
                WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN 'token'
                WHEN LOWER(token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN 'erc721'
                WHEN LOWER(token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN 'erc721'
                ELSE token_type
            END AS token_type,
            CASE 
                WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN SAFE_CAST(value AS NUMERIC)*POWER(1, -18)
                WHEN LOWER(token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN CAST(1 AS NUMERIC )
                WHEN LOWER(token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN CAST(1 AS NUMERIC )
                WHEN is_NFT = 1 THEN CAST(1 AS NUMERIC )
                WHEN decimals > 0 THEN SAFE_CAST(value AS NUMERIC)*POWER(1, -decimals)
                ELSE SAFE_CAST(value AS NUMERIC )
            END AS value_total
        FROM transactions  
        WHERE is_mapping = 1 OR LOWER(token_address) IN 
            (LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511'),
            LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7'),
            LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982')
            )
        )

    ,results AS
    (
        SELECT 
            user_address,
            token_address,
            address,
            url_symbol,
            token_name,
            token_type,
            symbol,
            decimals,
            offical_site,
            SUM(SAFE_CAST(value_total AS BIGNUMERIC)) AS value_total
        FROM data_token
        GROUP BY user_address,
            token_address,
            address,
            url_symbol,
            token_name,
            token_type,
            symbol,
            decimals,
            offical_site
    )

SELECT 
    results.*,
    ebl.eth_balance AS eth_balance
FROM results
LEFT JOIN `blockchain-etl-internal.common.ethereum_balances_live` AS ebl
ON LOWER(results.user_address) = LOWER(ebl.address)
