CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.raw_coingecko.coin_currency_{{ ds_nodash }}`
AS
SELECT
*
FROM `sipher-data-platform.raw_coingecko.coins_currency`
WHERE DATE(upload_time) = CURRENT_DATE()

