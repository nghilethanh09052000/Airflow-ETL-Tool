{{- config(
    materialized='table',
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
)-}}

WITH raw_generate AS (
    SELECT
        COALESCE({{ get_string_value_from_event_params(key="recipe_name") }}     
                , {{ get_string_value_from_event_params(key="recipe_id") }}
        ) AS recipe
        , ARRAY(
            SELECT 
            AS STRUCT
                ep.key AS key,
                ep.value.string_value AS value
            FROM UNNEST(event_params) AS ep 
            WHERE ep.key LIKE "%prompt%" OR ep.key LIKE "%Prompt%"
        ) AS prompt
        , ARRAY(
            SELECT 
            AS STRUCT
                ep.key AS key,
                ep.value.string_value AS value
            FROM UNNEST(event_params) AS ep 
            WHERE ep.key LIKE "%upload%" OR ep.key LIKE "%Upload%"
        ) AS upload
        , {{ get_string_value_from_event_params(key="image") }} AS image 
        , {{ get_string_value_from_event_params(key="aspectRatio") }} AS aspect_ratio
        , {{ get_string_value_from_event_params(key="numberOfImages") }} AS number_of_images
        , ROUND({{ get_int_value_from_event_params(key="engagement_time_msec") }} /1000,3) AS engagement_time_sec
        , version
        , page
        , date
        , CASE WHEN user_id = 'anonymous' THEN user_pseudo_id ELSE user_id END AS user_id
    FROM {{ ref("fct_artventure_user_events") }}
    WHERE event_name = 'click'
        AND {{ get_string_value_from_event_params(key="event_label") }} 
            IN ('recipe_alpha_generate', 'recipe-generate')
)
,reporting AS (
    SELECT
        * 
        , (SELECT COUNT(p.key) FROM UNNEST(prompt) AS p) AS count_prompt
        , (SELECT COUNT(u.key) FROM UNNEST(upload) AS u) AS count_upload_file
    FROM raw_generate
    WHERE recipe IS NOT NULL
)

SELECT * FROM reporting
