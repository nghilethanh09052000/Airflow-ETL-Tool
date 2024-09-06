{{- config(
    materialized='incremental',
)-}}

WITH
    data_token AS 
    (
        SELECT 
            *
        FROM {{ ref('stg_bq_public_ethereum__token_transfer_today')}}
        UNION ALL 
        SELECT 
            *
        FROM {{ ref('stg_bq_public_ethereum__token_transfer_last_15d')}}
    )

    ,data_single_side_deposit AS 
    (SELECT
      *,
      'deposit' AS tx_type,
      '$Sipher' AS token_type,
      'single_side' AS pool_type
    FROM data_token
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}'
      AND LOWER(to_address) = '{{ var("staked_sipher_pool_address")}}')
      OR
      (LOWER(from_address) = '{{ var("dummy_wallet_address")}}'
      AND LOWER(token_address) = '{{ var("staked_sipher_pool_address")}}'))
    )

  ,data_single_side_withdraw AS 
    (SELECT
      *,
      'withdraw' AS tx_type,
      '$Sipher' AS token_type,
      'single_side' AS pool_type
    FROM data_token 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}'
      AND LOWER(from_address) = '{{ var("staked_sipher_pool_address")}}')
      OR
      (LOWER(to_address) = '{{ var("dummy_wallet_address")}}'
      AND LOWER(token_address) = '{{ var("staked_sipher_pool_address")}}'))
    AND LOWER(to_address) != '{{ var("escrowed_sipher_address")}}'
    )

  ,data_single_side_claim_rewards AS 
    (SELECT
      *,
      'rewards' AS tx_type,
      '$Sipher' AS token_type,
      'single_side' AS pool_type
    FROM data_token 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}'
      AND LOWER(from_address) = '{{ var("staked_sipher_pool_address")}}'
      AND LOWER(to_address) = '{{ var("escrowed_sipher_address")}}')
      OR
      (LOWER(from_address) = '{{ var("dummy_wallet_address")}}'
      AND LOWER(token_address) = '{{ var("escrowed_sipher_address")}}'))
    )

  ,data_ls_uniswap_deposit AS 
    (SELECT
      *,
      'deposit' AS tx_type,
      CASE
        WHEN token_address = '{{ var("sipher_token_wallet_address") }}' THEN '$Sipher'
        WHEN token_address = '{{ var("wrapped_ether_address")}}' THEN 'WETH-Uni'
      END AS token_type,
      'uniswap' AS pool_type,
    FROM data_token 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}'
      AND LOWER(to_address) = '{{ var("uniswap_v2_sipher_address")}}')
      OR
      (LOWER(from_address) = '{{ var("uniswap_v2_sipher_address")}}'
      AND LOWER(token_address) = '{{ var("wrapped_ether_address")}}'))
    )

  ,data_ls_uniswap_withdraw AS 
    (SELECT
      *,
      'withdraw' AS tx_type,
      CASE
        WHEN token_address = '{{ var("sipher_token_wallet_address") }}' THEN '$Sipher'
        WHEN token_address = '{{ var("wrapped_ether_address")}}' THEN 'WETH-Uni'
      END AS token_type,
      'uniswap' AS pool_type
    FROM data_token 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}')
      AND LOWER(from_address) = '{{ var("uniswap_v2_sipher_address")}}')
      OR
      (LOWER(to_address) = '{{ var("uniswap_v2_sipher_address")}}'
      AND LOWER(token_address) = '{{ var("wrapped_ether_address")}}')
    )

  ,data_ls_kyberswap_deposit AS 
    (SELECT
      *,
      'deposit' AS tx_type,
      CASE
        WHEN token_address = '{{ var("sipher_token_wallet_address") }}' THEN '$Sipher'
        WHEN token_address = '{{ var("wrapped_ether_address")}}' THEN 'WETH-Uni'
      END AS token_type,
      'kyberswap' AS pool_type
    FROM data_token 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}'
      AND LOWER(to_address) = '{{ var("kyberswap_DMM_LP_sipher_address")}}')
      OR
      (LOWER(from_address) = '{{ var("kyberswap_DMM_LP_sipher_address")}}'
      AND LOWER(token_address) = '{{ var("wrapped_ether_address")}}'))
    )

  ,data_ls_kyberswap_withdraw AS 
    (SELECT
      *,
      'withdraw' AS tx_type,
      CASE
        WHEN token_address = '{{ var("sipher_token_wallet_address") }}' THEN '$Sipher'
        WHEN token_address = '{{ var("wrapped_ether_address")}}' THEN 'WETH-Uni'
      END AS token_type,
      'kyberswap' AS pool_type
    FROM data_token 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = '{{ var("sipher_token_wallet_address") }}'
      AND LOWER(from_address) = '{{ var("kyberswap_DMM_LP_sipher_address")}}')
      OR
      (LOWER(to_address) = '{{ var("kyberswap_DMM_LP_sipher_address")}}'
      AND LOWER(token_address) = '{{ var("wrapped_ether_address")}}'))
    )

  , all_data AS(
    SELECT 
      *,
      RANK() OVER (PARTITION BY transaction_hash ORDER BY log_index) AS hash_count
    FROM data_single_side_deposit
    UNION ALL
    SELECT 
      *,
      RANK() OVER (PARTITION BY transaction_hash ORDER BY log_index) AS hash_count
    FROM data_single_side_withdraw
    UNION ALL
    SELECT 
      *,
      RANK() OVER (PARTITION BY transaction_hash ORDER BY log_index) AS hash_count
    FROM data_single_side_claim_rewards
    UNION ALL
    SELECT 
      *,
      RANK() OVER (PARTITION BY transaction_hash ORDER BY log_index) AS hash_count
    FROM data_ls_uniswap_deposit
    UNION ALL
    SELECT 
      *,
      RANK() OVER (PARTITION BY transaction_hash ORDER BY log_index) AS hash_count
    FROM data_ls_uniswap_withdraw
    UNION ALL
    SELECT 
      *,
      RANK() OVER (PARTITION BY transaction_hash ORDER BY log_index) AS hash_count
    FROM data_ls_kyberswap_deposit
    UNION ALL
    SELECT 
      *,
      RANK() OVER (PARTITION BY transaction_hash ORDER BY log_index) AS hash_count
    FROM data_ls_kyberswap_withdraw
    )

SELECT 
*
FROM all_data