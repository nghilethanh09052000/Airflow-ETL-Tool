{{- config(
  materialized='table'
) -}}

WITH game_data AS (
    SELECT
        user_id AS game_user_id,
        MIN(event_timestamp) AS game_day0_datetime_tzutc
    FROM {{ ref('int_sipher_odyssey_login') }}
    GROUP BY ALL
)

,get_game_username AS (
    SELECT *
    FROM (
        SELECT
            DISTINCT
            user_id,
            JSON_EXTRACT_SCALAR(DATA, '$.Name') AS game_user_name,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY updated_at DESC) AS rk
        FROM `sipher-data-platform.raw_game_meta.sipher_prod_UserData`
        WHERE TRUE
            AND updated_at_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)
            AND JSON_EXTRACT_SCALAR(DATA, '$.Name') IS NOT NULL
        )
    WHERE rk=1
)

,user_email AS (
    SELECT
        DISTINCT
        user_id,
        user_last_modified_date,
        email,
        name,
        UNIX_MICROS(user_create_date) AS user_create_date
    FROM {{ ref('stg_aws__ather_id__raw_cognito') }}
)

,email_last_updated AS (
    SELECT
        user_id AS ather_id,
        MAX(user_last_modified_date) AS timestamp_date
    FROM user_email
    GROUP BY 1
)

,email_unique AS (
    SELECT
        DISTINCT
        elu.*,
        email,
        ue.name AS user_name,
        user_create_date
    FROM email_last_updated AS elu
    LEFT JOIN user_email AS ue
        ON elu.ather_id = ue.user_id
        AND elu.timestamp_date = ue.user_last_modified_date
)

,join_data AS (
    SELECT
        DISTINCT
        ather_id,
        user_name AS ather_user_name,
        email,
        user_create_date AS ather_created_timestamp,
    FROM email_unique
)

SELECT
    DISTINCT
    a.ather_id,
    email,
    b.ather_user_name,
    ather_created_timestamp,
    a.game_user_id,
    game_day0_datetime_tzutc,
    game_user_name,
FROM {{ ref('int_sipher_odyssey_game_user_lifespan') }} AS a
LEFT JOIN join_data AS b ON a.ather_id = b.ather_id
LEFT JOIN game_data AS c ON a.game_user_id = c.game_user_id
LEFT JOIN get_game_username AS d ON a.game_user_id = d.user_id
WHERE is_active = 1
