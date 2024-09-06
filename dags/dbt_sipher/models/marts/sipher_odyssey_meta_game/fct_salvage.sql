--

CREATE OR REPLACE TABLE `sipher-data-platform.reporting_game_meta.fct_salvage`
PARTITION BY event_timestamp
CLUSTER BY day_diff
AS

WITH raw AS
(
	SELECT  
        event_timestamp, 
        user_id, 
        event_name, 
        event_params, 
        app_info,
        user_pseudo_id 
	FROM {{ ref('stg_firebase__sipher_odyssey_events_all_time') }}
	WHERE true  
        AND _TABLE_SUFFIX > '0901'  
	    AND event_name IN ('salvage_action')
) 

, fct_user_login AS
(
    SELECT DISTINCT 
        user_id,
        day0_date_tzutc,
        build_number,
        app_version,
        email,
        user_name,
        current_build_timestamp,
        next_build_timestamp,
        pack_name,
        Date_Added,
        a.group,
    FROM `sipher-data-platform.reporting_game_level_design.fct_user_login` a

) 
 
, salvage_raw AS
(
	SELECT DISTINCT  
	    event_timestamp
	    ,user_pseudo_id
	    ,raw.user_id AS user_id
        ,(SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'reward') AS reward 
        ,(SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'sub_screen') AS screen 
	    ,(SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'item_type') AS item_type  
 	    ,(SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'instance_id') AS instance_id
	    ,(SELECT value.double_value FROM UNNEST (event_params) WHERE key = 'stackable') AS stackable
	FROM raw 
	WHERE event_name IN ('salvage_action')

)

, reformatjson AS
( 
    SELECT 
        * EXCEPT(screen,reward),
        REPLACE(reward, 'Chips', '"Chips"')reward,
--b1 thêm " vào sau { và trước }
--b2 thêm " vào trước và sau :
--b3 thêm " vào trước và sau , 
        REPLACE(REPLACE(REPLACE(REPLACE(screen, '{', '{"'), '}', '"}'), ': ', '":"'), ', ', '","')screen,

    FROM salvage_raw
)

,extract_json AS 
(
    SELECT DISTINCT 
        * EXCEPT(reward, screen),
        CAST(JSON_EXTRACT_SCALAR(reward, '$.Chips') AS INT64) Chips,
        JSON_EXTRACT_SCALAR(screen, '$.screen')screen,
        JSON_EXTRACT_SCALAR(screen, '$.tab')tab,
    FROM
    reformatjson
    )


,mapp_dim_user AS 
(
    SELECT 
        DATE(TIMESTAMP_MICROS(event_timestamp))event_timestamp,
        e.*EXCEPT(event_timestamp), 
        c.*EXCEPT(user_id, current_build_timestamp, next_build_timestamp),
        DATE_DIFF(DATE(TIMESTAMP_MICROS(event_timestamp)), DATE(day0_date_tzutc), DAY) day_diff,
    FROM extract_json e 
    JOIN fct_user_login c 
  	ON e.user_id = c.user_id AND e.event_timestamp > c.current_build_timestamp AND e.event_timestamp < COALESCE(c.next_build_timestamp, 7258118400000000)

)

SELECT * FROM mapp_dim_user