{{- config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    unique_key = ['event_timestamp','user_id'],
    merge_update_columns = [
    'action'
    , 'value'
    , 'value_tiers'
    , 'value_rarity'
    , 'value_rangedtypes'
    , 'value_geartype'
    , 'value_item_score'
    , 'value_sort_type'
    , 'sub_screen'
    , 'sub_screen_tab'
    , 'mode'
    , 'reward_chips'
    , 'reward_bytes'
    , 'item_type'
    , 'item_id'
    , 'instance_id'
    , 'stackable'
  ],
    partition_by={
        'field': 'date_tzutc',
        'data_type': 'DATE',
    },
)-}}
WITH raw AS
(
    SELECT  *
    FROM {{ ref('stg_firebase__sipher_odyssey_events_14d') }}
)
---### armory_action
, am AS (
    SELECT
    event_date
    , event_timestamp
    , event_name
    , user_id
    , ({{ get_string_value_from_event_params(key="action")}}) AS action
    , ({{ get_string_value_from_event_params(key="value")}})  AS value
    , CAST(NULL AS STRING)  AS value_tiers
    , CAST(NULL AS STRING)  AS value_rarity
    , CAST(NULL AS STRING)  AS value_rangedtypes
    , CAST(NULL AS STRING)  AS value_geartype
    , CAST(NULL AS STRING)  AS value_item_score
    , CAST(NULL AS STRING)  AS value_sort_type
    , ({{ get_string_value_from_event_params(key="sub_screen")}}) AS sub_screen
    , CAST(NULL AS STRING)  AS sub_screen_tab
    , CAST(NULL AS STRING)  AS mode
    , CAST(NULL AS INT64)   AS reward_chips
    , CAST(NULL AS INT64)   AS reward_bytes
    , CAST(NULL AS STRING)  AS item_type
    , CAST(NULL AS STRING)  AS item_id
    , CAST(NULL AS STRING)  AS instance_id
    , CAST(NULL AS FLOAT64) AS stackable
    , device.category       AS category
    , app_info.version      AS app_version
    , platform
    , UPPER(geo.country)    AS country
    FROM raw
    WHERE event_name IN ("armory_action")
)
---### salvage_action
, sa AS (
    SELECT
    event_date
    , event_timestamp
    , event_name
    , user_id
    ,               ({{  get_string_value_from_event_params(key="action"     ) }}                     ) AS action
    ,               ({{  get_string_value_from_event_params(key="value"      ) }}                     ) AS value
    , REGEXP_REPLACE(({{ get_string_value_from_event_params(key="sub_screen" )}}), r'(\w+)', '"\\1"'  ) AS sub_screen
    ,               ({{  get_string_value_from_event_params(key="mode"       ) }}                     ) AS mode
    , REGEXP_REPLACE(({{ get_string_value_from_event_params(key="reward"     )}}), r'(\w+):', '"\\1":') AS reward
    ,               ({{  get_string_value_from_event_params(key="item_type"  ) }}                     ) AS item_type
    ,               ({{  get_string_value_from_event_params(key="instance_id") }}                     ) AS instance_id
    ,               ({{  get_double_value_from_event_params(key="stackable"  ) }}                     ) AS stackable
    , device.category AS category
    , app_info.version AS app_version
    , platform
    , UPPER(geo.country) AS country
    FROM raw
    WHERE event_name IN ("salvage_action")
)
, sa_end as (
    SELECT
    event_date
    , event_timestamp
    , event_name
    , user_id
    , action
    , value
    , CAST(NULL AS STRING)  AS value_tiers
    , CAST(NULL AS STRING)  AS value_rarity
    , CAST(NULL AS STRING)  AS value_rangedtypes
    , CAST(NULL AS STRING)  AS value_geartype
    , CAST(NULL AS STRING)  AS value_item_score
    , CAST(NULL AS STRING)  AS value_sort_type
    , JSON_EXTRACT_SCALAR(sub_screen, '$.screen') AS sub_screen
    , JSON_EXTRACT_SCALAR(sub_screen, '$.tab')    AS sub_screen_tab
    , mode
    , CAST(JSON_EXTRACT_SCALAR(reward, '$.Chips') AS INT64) AS reward_chips
    , CAST(JSON_EXTRACT_SCALAR(reward, '$.Bytes') AS INT64) AS reward_bytes
    , item_type
    , CAST(NULL AS STRING)  AS item_id
    , instance_id
    , stackable
    , category
    , app_version
    , platform
    , country
    FROM sa
)
---### crafting_click
, cr AS (
  SELECT
    event_date
    , event_timestamp
    , event_name
    , user_id
    , ({{ get_string_value_from_event_params(key="action")}}) AS action
    , ({{ get_string_value_from_event_params(key="value")}})  AS value
    , REGEXP_REPLACE(({{ get_string_value_from_event_params(key="sub_screen")}}), r'(\w+):', '"\\1":') AS sub_screen
    , device.category  AS category
    , app_info.version AS app_version
    , platform
    , UPPER(geo.country) AS country
  FROM raw
  WHERE event_name IN ("crafting_click")
)
, af AS (
    SELECT
    event_date
    , event_timestamp
    , user_id
    , action
    , value
    , CAST(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(REGEXP_REPLACE(value, r'(\w+): ({[^}]+})', '"\\1": "\\2"'), '$.tiers'), '[{}]', '')  AS STRING) AS value_tiers
    , REGEXP_REPLACE(JSON_EXTRACT_SCALAR(REGEXP_REPLACE(value, r'(\w+): ({[^}]+})', '"\\1": "\\2"'), '$.rarity'), '[{}]', '')                 AS value_rarity
    , REGEXP_REPLACE(JSON_EXTRACT_SCALAR(REGEXP_REPLACE(value, r'(\w+): ({[^}]+})', '"\\1": "\\2"'), '$.rangedtypes'), '[{}]', '')            AS value_rangedtypes
    , REGEXP_REPLACE(JSON_EXTRACT_SCALAR(REGEXP_REPLACE(value, r'(\w+): ({[^}]+})', '"\\1": "\\2"'), '$.geartype'), '[{}]', '')               AS value_geartype
    FROM cr
    WHERE action = "apply_filter"
)
, sc AS (
    SELECT
    event_date
    , event_timestamp
    , user_id
    , action
    , value
    , REGEXP_EXTRACT(value, r'\[(.*),')     AS value_item_score
    , REGEXP_EXTRACT(value, r',\s*(\w+)\]') AS value_sort_type
    FROM cr
    WHERE action = "sort_click"
), sc_final AS (
    SELECT
    cr.event_date
    , cr.event_timestamp
    , cr.event_name
    , cr.user_id
    , cr.action
    , cr.value
    , af.value_tiers
    , af.value_rarity
    , af.value_rangedtypes
    , af.value_geartype
    , sc.value_item_score
    , sc.value_sort_type
    , JSON_EXTRACT_SCALAR(cr.sub_screen, '$.screen') AS sub_screen
    , JSON_EXTRACT_SCALAR(cr.sub_screen, '$.tab')    AS sub_screen_tab
    , CAST(NULL AS STRING)  AS mode
    , CAST(NULL AS INT64)   AS reward_chips
    , CAST(NULL AS INT64)   AS reward_bytes
    , CAST(NULL AS STRING)  AS item_type
    , CAST(NULL AS STRING)  AS item_id
    , CAST(NULL AS STRING)  AS instance_id
    , CAST(NULL AS FLOAT64) AS stackable
    , cr.category
    , cr.app_version
    , cr.platform
    , cr.country
    FROM cr
    LEFT JOIN af
    ON cr.event_timestamp = af.event_timestamp AND cr.user_id = af.user_id AND cr.action = af.action AND cr.value = af.value
    LEFT JOIN sc
    ON cr.event_timestamp = sc.event_timestamp AND cr.user_id = sc.user_id AND cr.action = sc.action AND cr.value = sc.value
)
---### crafting_action
, ca AS (
    SELECT
    event_date
    , event_timestamp
    , event_name
    , user_id
    , ({{ get_string_value_from_event_params(key="action")}}) AS action
    , ({{ get_string_value_from_event_params(key="value")}})  AS value
    , CAST(NULL AS STRING)  AS value_tiers
    , CAST(NULL AS STRING)  AS value_rarity
    , CAST(NULL AS STRING)  AS value_rangedtypes
    , CAST(NULL AS STRING)  AS value_geartype
    , CAST(NULL AS STRING)  AS value_item_score
    , CAST(NULL AS STRING)  AS value_sort_type
    , JSON_EXTRACT_SCALAR(({{ get_string_value_from_event_params(key="sub_screen")}}), '$.screen') AS sub_screen
    , JSON_EXTRACT_SCALAR(({{ get_string_value_from_event_params(key="sub_screen")}}), '$.tab')    AS sub_screen_tab
    , CAST(NULL AS STRING)  AS mode
    , CAST(NULL AS INT64)   AS reward_chips
    , CAST(NULL AS INT64)   AS reward_bytes
    , ({{ get_string_value_from_event_params(key="item_type")}}) AS item_type
    , ({{ get_string_value_from_event_params(key="item_id")}})   AS item_id
    , CAST(NULL AS STRING)  AS instance_id
    , CAST(NULL AS FLOAT64) AS stackable
    , device.category  AS category
    , app_info.version AS app_version
    , platform
    , UPPER(geo.country) AS country
    FROM raw
    WHERE event_name IN ("crafting_action")
)
, fin AS (
    SELECT * FROM am
    UNION ALL
    SELECT * FROM sa_end
    UNION ALL
    SELECT * FROM sc_final
    UNION ALL
    SELECT * FROM ca
)
, final_fct_crafting AS (
    SELECT PARSE_DATE('%Y%m%d', event_date) AS date_tzutc, * FROM fin ORDER BY 1
)
SELECT * FROM final_fct_crafting