{{- config(
    materialized='incremental',
    partition_by={
        "field": "record_date",
        "data_type": "DATE",
    },
) -}}

SELECT
    PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS record_date,
    wallet_address,
    max_act_date,
    accum_inu,
    accum_neko,
    accum_token
FROM `sipher-data-platform.sipher_ethereum.sipher_token_onwer_by_time_*` 
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_ADD('{{ var("ds")}}', INTERVAL -1 DAY))
 