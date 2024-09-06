{{
  config(
    materialized = 'table',
    partition_by={
        'field': 'date',
        'data_type': 'date',
    }
  )
}}

{%- set events = dbt_utils.get_column_values(
    table=ref('fct_artventure_page_events'),
    column='event_name'
)-%}

WITH mart AS (
    SELECT
        date
        , page
        , page_title
        , COUNT(DISTINCT user_id)       AS user_count
        , COUNT(DISTINCT ga_session_id) AS session_count
        {%- for event in events -%}
            , SUM(CASE WHEN event_name = '{{ event }}' THEN 1 ELSE 0 END) AS {{ event }}_count
        {% endfor -%}
    FROM {{ ref('fct_artventure_page_events') }}
    GROUP BY ALL
)

SELECT * FROM mart