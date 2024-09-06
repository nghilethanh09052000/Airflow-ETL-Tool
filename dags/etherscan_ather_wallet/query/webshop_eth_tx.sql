CREATE OR REPLACE TABLE `sipher-data-platform.raw_xsolla.webshop_eth_tx_{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}` AS

SELECT  
  *
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE TIMESTAMP_TRUNC(block_timestamp, DAY) =  TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND 
    (LOWER(to_address) = LOWER('0xb1d08A5cb3c3fc2570D323D7ce205c4365F52723')
    OR LOWER(from_address) = LOWER('0xb1d08A5cb3c3fc2570D323D7ce205c4365F52723'))

