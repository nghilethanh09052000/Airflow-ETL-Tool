{{- config(    
    materialized='table',
)-}}

WITH report AS (
    SELECT        
        date        
        , CASE WHEN user_id = 'anonymous' OR user_id IS NULL THEN user_pseudo_id ELSE user_id END AS user_id        
        , version 
        , page
        , {{ get_int_value_from_event_params(key="ga_session_id") }} AS session_id       
        , ( timestamp_diff( MAX(timestamp), MIN(timestamp), SECOND)) AS session_time_sec     
    FROM {{ ref("fct_artventure_user_events") }}   
    GROUP BY ALL
)

SELECT 
    * 
    , (session_time_sec/86400) as session_time_day
FROM report