{{- config(
  materialized='table',
) -}}


{%- set inu_legendary = [
  'MadJaxWarrior', 
  'WanderingSamurai', 
  'CosmicExplorer', 
  'MiniMe', 
  'GuardianOfOcean', 
  'CorruptedCosmic', 
  'Alucard', 
  'SipherEmployee',
  'Pharaoh',
  'Catstronaut'
] -%}

{%- set neko_legendary = [
  'CyberChuuni',
  'Rikku',
  'CopyCat',
  'Nekonaut',
  'Starfighter',
  'Cassasin',
  'PoisonCatnip',
  'OnnaMusha',
  'EgyptianGoddess',
  'ACE',
  'GeishaQueen'
] -%}

{%- set nonnft_common = [
  'NekoSuperNatural',
  'NekoCyborg',
  'InuElemental',
  'BuruCyborg',
  'BuruNormal',
  'NekoNormal',
  'BuruSuperNatural',
  'BuruElemental',
  'NekoElemental',
  'InuSuperNatural',
  'InuCyborg',
  'InuNormal'
] -%}


WITH raw AS (
    SELECT			
        instance_id AS item_id,				
        item_code,				
        item_type,				
        item_sub_type,				
        rarity,				
    FROM {{ ref('stg_sipher_server__raw_inventory_balancing_update') }}
    WHERE updated_balance_date >= "2024-03-08" AND item_type = 'Gear'
    GROUP BY 1,2,3,4,5
)

, a AS (
  SELECT  
    CASE
      WHEN REGEXP_CONTAINS(item_id, r'-[0-9]+-[0-9]+$' )       THEN REGEXP_REPLACE(item_id, '-[0-9]+-[0-9]+$', '')
      WHEN REGEXP_CONTAINS(item_id, r'[a-zA-Z0-9_]+-[0-9]+$' ) THEN REGEXP_REPLACE(item_id, '-[0-9]+$', '')
      ELSE NULL
    END AS item_id
    , item_code
    , item_type
    , item_sub_type
    , rarity
    , CASE 
        WHEN UPPER(item_id) LIKE '%INU%'  THEN "INU"
        WHEN UPPER(item_id) LIKE '%NEKO%' THEN "NEKO"
        WHEN UPPER(item_id) LIKE '%BURU%' THEN "BURU"
        ELSE NULL
      END AS race
  FROM raw 
)
, b AS (
  SELECT 
    item_id
    , SPLIT(item_id, '_')[OFFSET(0)] AS item_id_sub_type
    , CASE
        WHEN item_code = '' THEN item_id
        ELSE item_id
      END AS item_code
    , SPLIT(item_id, '_')[OFFSET(ARRAY_LENGTH(SPLIT(item_id, '_')) -2)] AS display
    , item_type
    , item_sub_type
    , rarity 
    , race
  FROM a
  GROUP BY ALL

)
, c AS (
  SELECT
    item_id
    ,item_id_sub_type
    , item_code
    , item_type
    , CASE
        WHEN display = 'GIFT' AND race = 'BURU' THEN 'Horn Hero'
        WHEN display = 'GIFT' AND race = 'INU'  THEN 'Wiggly Wag'
        WHEN display = 'GIFT' AND race = 'NEKO' THEN 'Yellow Yonder'
        WHEN display = 'EPIC' AND race = 'BURU' THEN 'BURU FTUE'
        WHEN display = 'EPIC' AND race = 'INU'  THEN 'INU FTUE'
        WHEN display = 'EPIC' AND race = 'NEKO' THEN 'NEKO FTUE'
        ELSE display
      END AS display_name
    ,item_sub_type
    , CASE
        WHEN display = 'GIFT' THEN "Uncommon"
        WHEN display = 'EPIC' THEN  'Epic'
        {% for _inu_legendary in inu_legendary %}
        WHEN item_id_sub_type = 'NFT' AND race = "INU"  AND display = '{{ _inu_legendary }}' THEN "Legendary"
        {% endfor -%}
        {% for _neko_legendary in neko_legendary %}
        WHEN item_id_sub_type = 'NFT' AND race = "NEKO" AND display = '{{ _neko_legendary }}'  THEN "Legendary"
        {% endfor -%}
        {% for _nonnft_common in nonnft_common %}
        WHEN item_id_sub_type = 'NonNFT' AND display = '{{ _nonnft_common }}'  THEN "Common"
        {% endfor -%}
        WHEN item_id_sub_type = 'NFT' THEN "rare"
        ELSE rarity
      END AS rarity
    ,race
  FROM b
)

SELECT
  item_id
  , item_id_sub_type
  , item_code
  , item_type
  , display_name
  , item_sub_type
  , MAX(rarity) OVER (PARTITION BY display_name, item_type, item_id_sub_type ) AS rarity
  , race
FROM c



