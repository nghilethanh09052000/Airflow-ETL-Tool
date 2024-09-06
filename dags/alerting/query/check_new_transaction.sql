WITH
tx AS
  (SELECT DISTINCT
    * EXCEPT(created_at, log_index)
  FROM `sipher-data-platform.sipher_ethereum.sipher_important_wallet_transactions`
  WHERE created_at BETWEEN DATE_SUB(CURRENT_DATETIME(), INTERVAL 30 MINUTE) AND CURRENT_DATETIME() )

,dim_wallet AS
  (SELECT
    DISTINCT wallet_address, name
  FROM `sipher-data-platform.sipher_ethereum.dim_sipher_wallet`)

,results AS 
(
  SELECT
    tx.token_address,
    tx.from_address,
    dim_from.name AS from_address_name,
    tx.to_address,
    dim_to.name AS to_address_name,
    tx.transaction_hash,
    tx.block_timestamp,
    COALESCE(etherscan.symbol, "Unknown token") AS symbol,
    COALESCE(CAST(value AS FLOAT64)/ POW(10, CAST(decimals AS INT64)), CAST(value AS FLOAT64)) AS converted_value
  FROM tx
  LEFT JOIN `sipher-data-platform.raw_etherscan.etherscan_tokens` AS etherscan
    ON LOWER(tx.token_address) = LOWER(etherscan.token_contract)
  LEFT JOIN dim_wallet AS dim_from ON LOWER(tx.from_address) = LOWER(dim_from.wallet_address)
  LEFT JOIN dim_wallet AS dim_to ON LOWER(tx.to_address) = LOWER(dim_to.wallet_address)
  ORDER BY block_timestamp DESC

)
SELECT
  *
FROM results
WHERE symbol <> "Unknown token"