{{- config(
  materialized='view'
) -}}

WITH pre_process_data AS(
    SELECT
        user_id,				
        instance_id,				
        item_code,				
        item_type,				
        item_sub_type,				
        rarity,				
        CAST(NULLIF(tier, '') AS INT64) AS tier,				
        CAST(NULLIF(level, '') AS INT64) AS level,				
        CAST(NULLIF(ps, '') AS INT64) AS ps,
        boost,				
        CAST(updated_balance AS INT64) AS updated_balance,
        updated_balance_timestamp,
        updated_balance_date,
        trace_id
    FROM {{ source('raw_game_meta', 'raw_inventory_balancing_update') }}
    WHERE updated_balance_date < CURRENT_DATE() AND updated_balance_date >= '2024-03-08'
)

SELECT
    user_id,				
    instance_id,				
    item_code,				
    item_type,
    item_sub_type,				
    rarity,
    tier,
    level,
    ps,
    boost,
    updated_balance,
    CASE
        WHEN LENGTH(updated_balance_timestamp) = 13 THEN CAST(updated_balance_timestamp AS INT64)*1000
        WHEN LENGTH(updated_balance_timestamp) = 16 THEN CAST(updated_balance_timestamp AS INT64)*10 - 62135596800000000
    END AS updated_balance_timestamp,
    updated_balance_date,
    trace_id
FROM pre_process_data