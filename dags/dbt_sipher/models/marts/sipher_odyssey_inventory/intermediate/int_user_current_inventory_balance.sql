{{- config(
  materialized='view'
) -}}

WITH staging_inventory_balance AS(
  SELECT * FROM {{ ref('stg_sipher_server__raw_inventory_balancing_update') }}
  UNION ALL
  SELECT * FROM {{ ref('stg_sipher_server__raw_inventory_balancing_update_today') }}
)

, instance_last_update_ts AS(
  SELECT
    user_id,
    instance_id,
    MAX(updated_balance_timestamp) AS updated_balance_timestamp
  FROM staging_inventory_balance
  -- WHERE updated_balance_date <= '2023-12-25'
  -- WHERE updated_balance_date BETWEEN DATE_ADD('2023-12-26', INTERVAL -3 DAY) AND '2023-12-26'
  GROUP BY user_id, instance_id
)

, instance_all_data_raw AS(
  SELECT DISTINCT
    user_id,
    instance_id,
    item_code,
    item_type,
    item_sub_type,
    rarity,
    MAX(tier) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp) AS tier,
    MAX(level) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp) AS level,
    MAX(ps) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp) AS ps,
    boost,
    MAX(updated_balance) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp) AS updated_balance,
    updated_balance_date,
    updated_balance_timestamp,
    trace_id
  FROM staging_inventory_balance 
  -- WHERE updated_balance_date <= '2023-12-25'
  -- WHERE updated_balance_date BETWEEN DATE_ADD('2023-12-26', INTERVAL -3 DAY) AND '2023-12-26'

  )

, instance_all_data AS(
  SELECT DISTINCT *
  FROM instance_all_data_raw
)

  SELECT DISTINCT
    instance_all_data.* EXCEPT(updated_balance_timestamp),
    TIMESTAMP_MICROS(updated_balance_timestamp) AS updated_balance_timestamp
  FROM instance_all_data
  INNER JOIN instance_last_update_ts USING(instance_id, updated_balance_timestamp, user_id)
  -- ORDER BY updated_balance_timestamp

--This model is capture the latest inventory balance update of each user updated on airflow - this does not includes historical balance
        