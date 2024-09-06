CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.raw_coinmarketcap.eth_quote`
AS
SELECT DISTINCT *
FROM `sipher-data-platform.raw_coinmarketcap.eth_quotes`