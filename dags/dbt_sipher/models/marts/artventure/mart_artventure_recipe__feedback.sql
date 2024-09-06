{{- config(
    materialized='table'
)-}}

{%- set responses = [
    'CLOSE',
    'HAPPY',
    'NEUTRAL',
    'SAD',
    'SKIP'
] -%}

WITH raw_feedback AS (
    SELECT
        CASE WHEN user_id = 'anonymous' THEN user_pseudo_id ELSE user_id END AS user_id,
        {{ get_string_value_from_event_params(key="recipe_id") }} AS recipe,
        {{ get_string_value_from_event_params(key="response") }} AS response
    FROM {{ ref("fct_artventure_user_events") }}
    WHERE event_name = 'click'
        AND {{ get_string_value_from_event_params(key="event_label") }} = "feedback"
)
,reporting AS (
    SELECT
        recipe,
        {%- for response in responses -%}
            SUM(CASE WHEN response = '{{ response }}' THEN 1 ELSE 0 END) AS cnt_{{ response }},
        {% endfor -%}
    FROM raw_feedback
    WHERE recipe IS NOT NULL
    GROUP BY recipe
)

SELECT * FROM reporting
