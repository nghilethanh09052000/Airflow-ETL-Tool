CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_ethereum.token_transaction_sipherians`
PARTITION BY DATE(block_timestamp) AS
WITH
    sipher_transactions AS 
    (
        SELECT 
            *
        FROM `sipher-data-platform.sipher_ethereum.sipher_token_transfers_*`
        UNION ALL 
        SELECT 
            *
        FROM `sipher-data-platform.sipher_ethereum.today_sipher_token_transfers`
    )

    ,sipher_data_filter AS 
    (
        SELECT
            from_address AS address,
            token_address,
            token_name,
            -CAST(sipher_value AS NUMERIC)  AS sipher_value
        FROM sipher_transactions
        UNION ALL
        SELECT
            to_address AS address,
            token_address,
            token_name,
            CAST(sipher_value AS NUMERIC)  AS sipher_value
        FROM sipher_transactions
    )

    ,sipherians AS 
    (
        SELECT
            address,
            SUM(sipher_value) AS sipher_value
        FROM sipher_data_filter
        GROUP BY 1
    )

    ,transactions AS
    (
        SELECT  
        *
        FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
        WHERE LOWER(from_address) IN (SELECT address FROM sipherians WHERE sipher_value > 0)
        OR LOWER(to_address) IN (SELECT address FROM sipherians WHERE sipher_value > 0)
    )

    ,transactions_join AS
    (
        SELECT  
        DATE(tt.block_timestamp) AS act_date,
        CASE 
                WHEN LOWER(tt.token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN 'SIPHER Token'
                WHEN LOWER(tt.token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN 'SIPHER INU'
                WHEN LOWER(tt.token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN 'SIPHER NEKO'
                WHEN LOWER(tt.token_address) = LOWER(t.address) THEN t.name
                ELSE 'N/A'
            END AS token_name,
        t.symbol AS symbol,
        t.decimals AS decimals,
        t.block_timestamp AS token_block_timestamp,
        t.block_number AS token_block_number,
        t.block_hash AS token_block_hash,
        t.total_supply AS total_supply,
        CASE 
                WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN CAST(value AS NUMERIC)/POWER(10, 18)
                WHEN LOWER(token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN CAST(value AS NUMERIC)
                WHEN LOWER(token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN CAST(value AS NUMERIC)
                ELSE 0
            END AS sipher_value,
        tt.*
    FROM transactions AS tt 
    LEFT JOIN `bigquery-public-data.crypto_ethereum.tokens`  t ON
    tt.token_address = t.address
    -- WHERE DATE(tt.block_timestamp) > '2021-09-01' --DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    -- AND LOWER(token_address) IN (LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982'), LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511'), LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7'))
    )

SELECT 
    *
FROM transactions_join