CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_ethereum.sipher_token_onwer_all_time` 
PARTITION BY record_date
AS
    SELECT
    PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS record_date,
    wallet_address,
    max_act_date,
    accum_inu,
    accum_neko,
    accum_token
    FROM `sipher-data-platform.sipher_ethereum.sipher_token_onwer_by_time_*` 
 