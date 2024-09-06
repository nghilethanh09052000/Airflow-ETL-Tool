{{
  config(
    materialized = 'table',
    )
}}

WITH raw AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date)    AS date
        , TIMESTAMP_MICROS(event_timestamp) AS timestamp
        , user_id
        , user_pseudo_id
        , {{ get_string_value_from_event_params(key='page_title') }}                 AS page_title
        , CAST({{ get_int_value_from_event_params(key='ga_session_id') }} AS STRING) AS ga_session_id
        , event_name
        , event_params
    FROM {{ ref('stg_firebase__artventure_events_all_time') }}
    WHERE 
        event_name NOT IN ('cli  ck', 'task_executed', 'task_executing','task_registered')
)

,page_events AS (
    SELECT
        *
        , CASE WHEN
            {{ get_string_value_from_event_params(key="page_location") }} like '%alpha%'
            THEN 'alpha' ELSE 'other'
        END AS version
        , REGEXP_EXTRACT({{ get_string_value_from_event_params(key="page_location") }}, r'://([^/]+)/') AS page
    FROM raw
    WHERE page_title IS NOT NULL
)

SELECT * FROM page_events