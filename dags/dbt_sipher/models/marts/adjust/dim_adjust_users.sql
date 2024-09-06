{{- config(
    materialized='table',
) -}}

WITH first AS (
    SELECT
        JSON_EXTRACT_SCALAR(publisher_parameters, '$.user_id') AS user_id
      , app_name_dashboard                                     AS app_name
      , environment
      , device_type
      , os_name
      , city
      , country
      , activity_kind
      , event_name
      , created_at
    FROM 
        {{ ref('stg_adjust')}}
    WHERE 
        JSON_EXTRACT_SCALAR(publisher_parameters, '$.user_id') IS NOT NULL
)
, dim AS (
SELECT 
  *
  , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at ) AS row_number
FROM first
)

SELECT 
    created_at    AS first_created_at
  , user_id
  , app_name
  , environment
  , device_type
  , os_name
  , activity_kind AS first_activity_kind
  , event_name    AS first_event_name
  , city
  , country
FROM  dim
WHERE row_number = 1



