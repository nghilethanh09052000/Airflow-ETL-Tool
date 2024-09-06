{{- config(
  materialized='table',
  
) -}}

{%- set weapon_melee = [
  'CHAINSAW'
  , 'KATANA'
  , 'IRONFIST'
  , 'LIGHTNINGWHIP'
  , 'SHIELD'
] -%}

{%- set weapon_range = [
  'ASSAULTRIFLE'
  , 'SHOTGUN'
  , 'DUALREVOLVER'
  , 'DUALSMG'
  , 'GRENADELAUNCHER'
  , 'RAYSHOOTERS'
  , 'SNIPER'
  , 'MACHINEGUN'
] -%}

WITH raw AS (
  SELECT			
    instance_id AS item_id,				
    item_code,				
    item_type,				
    item_sub_type,				
    rarity
  FROM {{ ref('stg_sipher_server__raw_inventory_balancing_update') }}
  WHERE updated_balance_date >= "2024-03-08" AND item_type = 'Weapon'
  GROUP BY 1,2,3,4,5
)
, trans AS (
  SELECT 
    CASE
      WHEN REGEXP_CONTAINS(item_id, r'-[0-9]+-[0-9]+$' )       THEN REGEXP_REPLACE(item_id, '-[0-9]+-[0-9]+$', '')
      WHEN REGEXP_CONTAINS(item_id, r'[a-zA-Z0-9_]+-[0-9]+$' ) THEN REGEXP_REPLACE(item_id, '-[0-9]+$', '')
      ELSE NULL
    END AS item_id
    , item_code
    , item_type
    , item_sub_type
    , CASE
        WHEN item_id LIKE 'FTUE03%' THEN 'Uncommon'
        WHEN item_id LIKE 'FTUE%' THEN 'Legendary'
        WHEN rarity IS NULL AND item_id LIKE "%_Uncommon_%" THEN "Uncommon"
        WHEN rarity IS NULL AND item_id LIKE "%_Default_%"  THEN "Common"
        WHEN rarity IS NULL AND item_id LIKE "%_Modular_%"  THEN "Common"
        WHEN rarity IS NULL AND item_id LIKE "%_Common_%"   THEN "Common"
        WHEN rarity IS NULL AND item_id LIKE "%_Rare_%"     THEN "Rare"
        WHEN rarity IS NULL AND item_id LIKE "%_Epic_%"     THEN "Epic"
        WHEN rarity IS NULL AND item_id LIKE "%_Mythic_%"   THEN "Mythic"
        WHEN rarity IS NULL AND item_id LIKE "%_Legend_%"   THEN "Legendary"
        ELSE rarity
    END AS rarity
    , CASE 
        WHEN UPPER(item_id) LIKE '%INU%'  THEN "INU"
        WHEN UPPER(item_id) LIKE '%NEKO%' THEN "NEKO"
        WHEN UPPER(item_id) LIKE '%BURU%' THEN "BURU"
        ELSE NULL
    END AS race
  FROM raw
)
, d as (
  SELECT
    item_id
    , CASE
        WHEN item_code = '' THEN item_id
        ELSE item_id
      END AS item_code
    , item_type
    , item_sub_type
    , CASE
        {% for _melee in weapon_melee %}
        WHEN UPPER(item_sub_type) = '{{ _melee }}' THEN "Melee"
        {% endfor -%}
        {% for _range in weapon_range %}
        WHEN UPPER(item_sub_type) = '{{ _range }}' THEN "Range"
        {% endfor -%}
        ELSE NULL
    END AS weapon_type
    , MAX(rarity) OVER (PARTITION BY item_id, item_type, item_sub_type ) AS rarity
    , race
  FROM trans
)

SELECT
  item_id
  , item_code
  , item_type
  , item_sub_type
  , weapon_type
  , rarity
  , race
FROM d
GROUP BY 1,2,3,4,5,6,7