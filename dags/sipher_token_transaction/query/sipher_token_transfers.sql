CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_ethereum.sipher_token_transfers_{{ ds_nodash }}`
PARTITION BY DATE(block_timestamp) AS
SELECT  
    DATE(block_timestamp) AS act_date,
    CASE 
            WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN 'SIPHER Token'
            WHEN LOWER(token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN 'SIPHER INU'
            WHEN LOWER(token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN 'SIPHER NEKO'
            ELSE 'N/A'
        END AS token_name,
    CASE 
            WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN CAST(value AS NUMERIC)
            WHEN LOWER(token_address) = LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7') THEN 1
            WHEN LOWER(token_address) = LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982') THEN 1
            ELSE 0
        END AS sipher_value,
    *
FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
WHERE DATE(block_timestamp) = '{{ ds }}' --DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
AND DATE(block_timestamp) < CURRENT_DATE()
AND LOWER(token_address) IN (LOWER('0x09E0dF4aE51111CA27d6B85708CFB3f1F7cAE982'), LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511'), LOWER('0x9c57D0278199c931Cf149cc769f37Bb7847091e7'))