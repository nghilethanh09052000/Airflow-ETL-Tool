{{- config(
    materialized='view',
    partition='act_date'
)-}}

SELECT  
    DATE(block_timestamp) AS date_tzutc,
    CASE 
            WHEN LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}' THEN 'SIPHER Token'
            WHEN LOWER(token_address) = '{{ var("sipher_inu_wallet_address") }}' THEN 'SIPHER INU'
            WHEN LOWER(token_address) = '{{ var("sipher_neko_wallet_address") }}' THEN 'SIPHER NEKO'
            ELSE 'N/A'
        END AS token_name,
    CASE 
            WHEN LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}' THEN CAST(value AS NUMERIC)
            WHEN LOWER(token_address) = '{{ var("sipher_inu_wallet_address") }}' THEN 1
            WHEN LOWER(token_address) = '{{ var("sipher_neko_wallet_address") }}' THEN 1
            ELSE 0
        END AS sipher_value,
    *
FROM {{ source('crypto_ethereum', 'token_transfers') }}
WHERE DATE(block_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND LOWER(token_address) IN ('{{ var("sipher_token_wallet_address") }}', '{{ var("sipher_inu_wallet_address") }}', '{{ var("sipher_neko_wallet_address") }}')