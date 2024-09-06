MERGE INTO `{{ params.bq_project }}.sipher_ethereum.sipher_important_wallet_transactions` AS t
USING (

SELECT
  CURRENT_DATETIME() AS created_at,
  *
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE TRUE
  AND DATE(block_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
  AND (
        `sipher-data-platform.udf.filter_addresses`(
          from_address,
          ARRAY((SELECT wallet_address FROM `sipher-data-platform.sipher_ethereum.dim_sipher_wallet` WHERE is_outflow_tracked))
        )
      OR
        `sipher-data-platform.udf.filter_addresses`(
          to_address,
          ARRAY((SELECT wallet_address FROM `sipher-data-platform.sipher_ethereum.dim_sipher_wallet` WHERE is_inflow_tracked))
        )
  )

UNION ALL

SELECT
  CURRENT_DATETIME() AS created_at,
  CAST(NULL AS STRING) AS token_address,
  from_address,
  to_address,
  CAST(value AS STRING) AS value,
  `hash` AS transaction_hash,
  transaction_index AS log_index,
  block_timestamp,
  block_number,
  block_hash
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE TRUE
  AND DATE(block_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
  AND (
        `sipher-data-platform.udf.filter_addresses`(
          from_address,
          ARRAY((SELECT wallet_address FROM `sipher-data-platform.sipher_ethereum.dim_sipher_wallet` WHERE is_outflow_tracked))
        )
      OR
        `sipher-data-platform.udf.filter_addresses`(
          to_address,
          ARRAY((SELECT wallet_address FROM `sipher-data-platform.sipher_ethereum.dim_sipher_wallet` WHERE is_inflow_tracked))
        )
  )

) AS s

ON t.transaction_hash = s.transaction_hash
AND t.log_index = s.log_index

WHEN NOT MATCHED THEN
    INSERT ROW
