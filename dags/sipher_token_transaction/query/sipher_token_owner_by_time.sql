CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_ethereum.sipher_token_onwer_by_time_{{ ds_nodash }}`
PARTITION BY max_act_date AS
WITH 
    data_token AS 
    (
        SELECT 
            *
        FROM `sipher-data-platform.sipher_ethereum.sipher_token_transfers_*`
        WHERE DATE(block_timestamp) <= '{{ ds }}'
    )

    ,token_pre_results AS 
    (
        SELECT
            act_date,
            block_timestamp,
            from_address AS address,
            token_address,
            token_name,
            -CAST(sipher_value AS NUMERIC)  AS sipher_value
        FROM data_token
        UNION ALL
        SELECT
            act_date,
            block_timestamp,
            to_address AS address,
            token_address,
            token_name,
            CAST(sipher_value AS NUMERIC)  AS sipher_value
        FROM data_token
    )
  
    ,token_results AS
    (
        SELECT
            act_date,
            block_timestamp,
            LOWER(address) AS wallet_address,
            -- token_name,
            SUM(CASE
                WHEN LOWER(token_name) = "sipher inu" THEN sipher_value
                ELSE 0
                END) AS INU_TOTAL,
            SUM(CASE
                WHEN LOWER(token_name) = "sipher neko" THEN sipher_value
                ELSE 0
                END) AS NEKO_TOTAL,
            SUM(CASE
                WHEN LOWER(token_name) = "sipher token" THEN sipher_value
                ELSE 0
                END) AS TOKEN_TOTAL
        FROM token_pre_results
        GROUP BY 1,2,3
    )

    ,rolling_sum AS(
        SELECT
            wallet_address,
            act_date,
            block_timestamp,
            SUM(INU_TOTAL)
            OVER (
                PARTITION BY wallet_address
                ORDER BY act_date
                ROWS UNBOUNDED PRECEDING) AS accum_inu,
            SUM(NEKO_TOTAL)
            OVER (
                PARTITION BY wallet_address
                ORDER BY act_date
                ROWS UNBOUNDED PRECEDING) AS accum_neko,
            SUM(TOKEN_TOTAL)
            OVER (
                PARTITION BY wallet_address
                ORDER BY act_date
                ROWS UNBOUNDED PRECEDING) AS accum_token,
        FROM token_results

        ORDER BY wallet_address, act_date)

    , final AS
    (
        SELECT
            wallet_address,
            block_timestamp,
            ROW_NUMBER() OVER (PARTITION BY wallet_address ORDER BY block_timestamp DESC) AS block_timestamp_order,
            MAX(act_date) OVER (PARTITION BY wallet_address) AS max_act_date,
            accum_inu,
            accum_neko,
            accum_token
        FROM rolling_sum
        ORDER BY 4 DESC,1)

    SELECT
        *
    FROM final
    WHERE block_timestamp_order = 1