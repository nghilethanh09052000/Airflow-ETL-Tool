CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.raw_staking.sipher_staking_rewards_claimed_and_withdrawn_{{ ds_nodash }}` AS


WITH raw_data AS 
(
    SELECT
        *
    FROM `bigquery-public-data.crypto_ethereum.token_transfers`
    WHERE DATE(block_timestamp) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
)

,claimed_tx_hash AS(
    SELECT
        DISTINCT transaction_hash
    FROM raw_data
    WHERE 
        (
        LOWER(from_address) = LOWER('0xB2d1464Ae4cc86856474a34d112b4A2efa326ed9')
        OR LOWER(to_address) = LOWER('0xB2d1464Ae4cc86856474a34d112b4A2efa326ed9')
        )
    )

,filtered_claimed_tx AS(
    SELECT
        DATE(block_timestamp) AS act_date,
        token_address,
        CASE
                WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN 'SIPHER Token'
                WHEN LOWER(token_address) = LOWER('0xb2d1464ae4cc86856474a34d112b4a2efa326ed9') THEN 'Escrowed SIPHER Token'
                ELSE 'N/A'
            END AS token_name,
        CAST(value AS FLOAT64)*POWER(10, -18) AS sipher_value,
        * EXCEPT(value, token_address)
    FROM raw_data
    WHERE transaction_hash IN (SELECT * FROM claimed_tx_hash)
)

, final AS
(
  SELECT  
    act_date,
    transaction_hash,
    from_address,
    to_address,
    token_address,
    token_name,
    CASE
      WHEN from_address = '0x0000000000000000000000000000000000000000' AND LOWER(token_address) = LOWER('0xb2d1464ae4cc86856474a34d112b4a2efa326ed9') THEN 'claimed'
      WHEN to_address = '0x0000000000000000000000000000000000000000' AND LOWER(token_address) = LOWER('0xb2d1464ae4cc86856474a34d112b4a2efa326ed9') THEN 'withdraw'
      ELSE 'N/A'
    END AS action_type,
    SUM(sipher_value) AS sipher_token
  FROM filtered_claimed_tx
  WHERE token_name <> 'N/A'
  GROUP BY 1,2,3,4,5,6

  --  LIMIT 1000
)

SELECT
  CASE 
    WHEN action_type = 'claimed' THEN to_address
    WHEN action_type = 'withdraw' THEN from_address
    ELSE 'N/A'
  END AS user_wallet,
  *
FROM final
WHERE token_name = 'Escrowed SIPHER Token'