{{- config(
    materialized='table'
) -}}

WITH raw AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date)    AS date
        , TIMESTAMP_MICROS(event_timestamp) AS timestamp
        , event_name
        , event_params
    FROM {{ ref('stg_firebase__artventure_events_all_time') }}
    WHERE event_name IN (
        'task_registered',
        'task_executing',
        'task_executed'
    )
)
,task_events AS (
    SELECT
        date
        , timestamp
        , {{ get_string_value_from_event_params(key="task_id") }} AS task_id
        , event_name AS status
        , {{ get_int_value_from_event_params(key="tasks_in_queue") }} AS tasks_in_queue
        , {{ get_int_value_from_event_params(key="queue_duration") }} AS queue_duration
        , {{ get_int_value_from_event_params(key="runpod_queue_duration") }} AS runpod_queue_duration
        , {{ get_int_value_from_event_params(key="execution_duration") }}    AS execution_duration
    FROM raw
)

SELECT * FROM task_events