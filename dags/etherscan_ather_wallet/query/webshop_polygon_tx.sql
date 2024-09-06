CREATE OR REPLACE TABLE `sipher-data-platform.raw_xsolla.webshop_polygon_tx_{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}` AS

SELECT 
  *
FROM `bigquery-public-data.crypto_polygon.token_transfers`
WHERE TIMESTAMP_TRUNC(block_timestamp, DAY) =  TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND 
    (LOWER(to_address) = LOWER('0x6EFE17902c4aA13ad5fc0dfeD36a214e1D65cdb2')
    OR LOWER(from_address) = LOWER('0x6EFE17902c4aA13ad5fc0dfeD36a214e1D65cdb2'))