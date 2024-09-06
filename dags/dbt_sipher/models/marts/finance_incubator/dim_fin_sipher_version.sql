{{- config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
	merge_update_columns = [
    'version',
	'to_date'
  ],
	partition_by={
		'field': 'from_date',
		'data_type': 'DATE',
	}
)-}}

WITH a AS (
    SELECT 
        app_info.version AS version
        , PARSE_DATE('%Y%m%d', event_date) AS ev_date
    FROM {{ ref('stg_firebase__sipher_odyssey_events_14d')}}
    WHERE FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) > '2024-03-07' 
        AND app_info.id != 'com.atherlabs.sipherodyssey1' 
        OR NOT (app_info.id = 'com.atherlabs.sipherodyssey' AND device.operating_system ='Android')
)
, b AS (
    SELECT 
        version
        , MIN(ev_date) AS from_date
    FROM a
    GROUP BY 1
)

, final AS (
    SELECT 
        version
        , from_date
        , LEAD(from_date, 1, CURRENT_DATE()) OVER (ORDER BY version) AS to_date
    FROM b
)

{% if is_incremental() -%}
, old_data AS (
    SELECT 
        MAX(version) AS max_version
        , MAX(from_date) AS from_date
        , MAX(to_date) AS max_to_date
    FROM {{ this }}
),
incremental_update AS (
    SELECT 
        f.version
        , CASE 
            WHEN f.version = od.max_version AND f.to_date > od.max_to_date THEN od.from_date
            ELSE f.from_date
        END AS from_date
        , CASE
            WHEN f.version = od.max_version AND (SELECT MAX(version) FROM final) > od.max_version THEN od.max_to_date
            ELSE f.to_date
        END AS to_date  
    FROM final f
    LEFT JOIN old_data od ON 1=1
    WHERE 
        f.version > od.max_version OR 
        (f.version = od.max_version AND f.to_date > od.max_to_date)
)

SELECT * FROM incremental_update

{% else %}

SELECT 
    version
    , from_date
    , to_date
FROM final

{% endif %}
