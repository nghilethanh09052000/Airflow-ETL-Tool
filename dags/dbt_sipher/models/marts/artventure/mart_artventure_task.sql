{{- config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
)-}}

{%- set statuses = [
    'task_registered',
    'task_executing',
    'task_executed',
] -%}


WITH fact AS (
    SELECT
        *
    FROM {{ ref("fct_artventure_task_events") }}
)
,reporting AS (
    SELECT
        date
        {%- for status in statuses -%}
            , SUM(CASE WHEN status = '{{ status }}' THEN 1 ELSE 0 END) AS {{ status }}
        {% endfor -%}
        , AVG(tasks_in_queue) / 1000 AS avg_tasks_in_queue
        , AVG(queue_duration) / 1000 AS avg_queue_duration
        , AVG(runpod_queue_duration) / 1000 AS avg_runpod_queue_duration
        , AVG(execution_duration) / 1000 AS avg_execution_duration
    FROM fact
    GROUP BY date
)

SELECT * FROM reporting