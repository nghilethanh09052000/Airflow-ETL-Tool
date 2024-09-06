MERGE INTO `data-analytics-342807.staking_public_data.staking_pool_transaction` AS historical_transactions
USING (
WITH 
  data_single_side_deposit AS 
    (SELECT
      *,
      'deposit' AS tx_type,
      '$Sipher' AS token_type,
      'single_side' AS pool_type
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = LOWER('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511')
      AND LOWER(to_address) = LOWER('0x7ee4b5dbc4b97c30a08791ce8601e695735960db'))
      OR
      (LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000')
      AND LOWER(token_address) = LOWER('0x7ee4b5dbc4b97c30a08791ce8601e695735960db')))
    )

  ,data_single_side_withdraw AS 
    (SELECT
      *,
      'withdraw' AS tx_type,
      '$Sipher' AS token_type,
      'single_side' AS pool_type
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = LOWER('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511')
      AND LOWER(from_address) = LOWER('0x7ee4b5dbc4b97c30a08791ce8601e695735960db'))
      OR
      (LOWER(to_address) = LOWER('0x0000000000000000000000000000000000000000')
      AND LOWER(token_address) = LOWER('0x7ee4b5dbc4b97c30a08791ce8601e695735960db')))
    AND LOWER(to_address) != LOWER('0xb2d1464ae4cc86856474a34d112b4a2efa326ed9')
    )

  ,data_single_side_claim_rewards AS 
    (SELECT
      *,
      'rewards' AS tx_type,
      '$Sipher' AS token_type,
      'single_side' AS pool_type
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = LOWER('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511')
      AND LOWER(from_address) = LOWER('0x7ee4b5dbc4b97c30a08791ce8601e695735960db')
      AND LOWER(to_address) = LOWER('0xb2d1464ae4cc86856474a34d112b4a2efa326ed9'))
      OR
      (LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000')
      AND LOWER(token_address) = LOWER('0xb2d1464ae4cc86856474a34d112b4a2efa326ed9')))

    )

  ,data_ls_uniswap_deposit AS 
    (SELECT
      *,
      'deposit' AS tx_type,
      CASE
        WHEN token_address = '0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511' THEN '$Sipher'
        WHEN token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'WETH-Uni'
      END AS token_type,
      'uniswap' AS pool_type,
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = LOWER('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511')
      AND LOWER(to_address) = LOWER('0xf3fdcfbfdb96315fc628854627bdd5e363b3ade4'))
      OR
      (LOWER(from_address) = LOWER('0xf3fdcfbfdb96315fc628854627bdd5e363b3ade4')
      AND LOWER(token_address) = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')))
    )

  ,data_ls_uniswap_withdraw AS 
    (SELECT
      *,
      'withdraw' AS tx_type,
      CASE
        WHEN token_address = '0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511' THEN '$Sipher'
        WHEN token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'WETH-Uni'
      END AS token_type,
      'uniswap' AS pool_type
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = LOWER('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511')
      AND LOWER(from_address) = LOWER('0xf3fdcfbfdb96315fc628854627bdd5e363b3ade4'))
      OR
      (LOWER(to_address) = LOWER('0xf3fdcfbfdb96315fc628854627bdd5e363b3ade4')
      AND LOWER(token_address) = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')))
    )

  ,data_ls_kyberswap_deposit AS 
    (SELECT
      *,
      'deposit' AS tx_type,
      CASE
        WHEN token_address = '0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511' THEN '$Sipher'
        WHEN token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'WETH-Uni'
      END AS token_type,
      'kyberswap' AS pool_type
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = LOWER('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511')
      AND LOWER(to_address) = LOWER('0x9A56f30fF04884cB06da80cB3aEf09c6132f5E77'))
      OR
      (LOWER(from_address) = LOWER('0x9A56f30fF04884cB06da80cB3aEf09c6132f5E77')
      AND LOWER(token_address) = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')))
    )

  ,data_ls_kyberswap_withdraw AS 
    (SELECT
      *,
      'withdraw' AS tx_type,
      CASE
        WHEN token_address = '0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511' THEN '$Sipher'
        WHEN token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'WETH-Uni'
      END AS token_type,
      'kyberswap' AS pool_type
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
    WHERE  
      DATE(block_timestamp) BETWEEN CURRENT_DATE()-2 AND CURRENT_DATE() 
    AND
      ((LOWER(token_address) = LOWER('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511')
      AND LOWER(from_address) = LOWER('0x9A56f30fF04884cB06da80cB3aEf09c6132f5E77'))
      OR
      (LOWER(to_address) = LOWER('0x9A56f30fF04884cB06da80cB3aEf09c6132f5E77')
      AND LOWER(token_address) = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')))
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
) AS new_transactions
    ON historical_transactions.transaction_hash = new_transactions.transaction_hash 
    AND historical_transactions.block_hash = new_transactions.block_hash 
    AND historical_transactions.block_timestamp = new_transactions.block_timestamp 
    AND historical_transactions.log_index = new_transactions.log_index
    
    WHEN NOT MATCHED THEN INSERT
    (`token_address`, `from_address`, `to_address`, `value`, `transaction_hash`, `log_index`, `block_timestamp`, `block_number`, `block_hash`, `tx_type`, `pool_type`, `hash_count`)
    VALUES
    (`token_address`, `from_address`, `to_address`, `value`, `transaction_hash`, `log_index`, `block_timestamp`, `block_number`, `block_hash`, `tx_type`, `pool_type`, `hash_count`)
