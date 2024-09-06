{{ config(materialized='table') }}

WITH users_with_cohorts AS (
    SELECT
        EXTRACT(
            MONTH
            FROM
                CAST(event_time AS DATE)
        ) AS cohort,
        user_id,
        event_time
    FROM
        `sipher-odyssey.pc_game_analytics.sipher_pc_event`
),
user_activity AS (
    SELECT
        user_id,
        event_time AS active_at
    FROM
        `sipher-odyssey.pc_game_analytics.sipher_pc_init`
    WHERE
        user_id IS NOT NULL
),
activity_days_per_user AS (
    SELECT
        c.user_id,
        cohort,
        DATE_DIFF(a.active_at, c.event_time, DAY) AS activity_day
    FROM
        users_with_cohorts c
        LEFT JOIN user_activity a ON c.user_id = a.user_id
)
SELECT
    cohort,
    COUNT(
        DISTINCT(CASE WHEN activity_day = 1 THEN user_id END)
    ) AS D1,
    COUNT(
        DISTINCT(CASE WHEN activity_day = 2 THEN user_id END)
    ) AS D2,
    COUNT(
        DISTINCT(CASE WHEN activity_day = 7 THEN user_id END)
    ) AS D7,
    COUNT(
        DISTINCT(CASE WHEN activity_day = 30 THEN user_id END)
    ) AS D30
FROM
    activity_days_per_user
GROUP BY
    cohort