{{- config(
  materialized='table',
) -}}

WITH raw AS (
    SELECT			
        instance_id AS item_id		
        , item_code			
        , item_type				
        , item_sub_type				
        , rarity				
    FROM {{ ref('stg_sipher_server__raw_inventory_balancing_update') }}
    WHERE updated_balance_date >= "2024-03-08" AND item_type = 'EM Shards'
    GROUP BY 1,2,3,4,5
)
, shard AS (
    SELECT
       item_id
       , item_code
       , item_type
       , rarity
       , SPLIT(item_id, '_')[OFFSET(0)] AS race
    FROM raw
)

SELECT * FROM shard