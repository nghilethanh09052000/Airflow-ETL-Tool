{{config(materialized='table')}}

WITH nomean AS (
    SELECT
        'no' nm
),
raw as (
    SELECT
        e.user_id,
        DATETIME(FORMAT_DATE('%F %T', e.event_time)) event_time,
        DATE(FORMAT_DATE('%F', e.event_time)) event_date,
        e.event_name,
        e.event_parameter,
        device_info.os AS device_os,
        geo.country AS country
    FROM
        `sipher-odyssey.pc_game_analytics.sipher_pc_event` e
        LEFT JOIN `sipher-odyssey.pc_game_analytics.sipher_pc_init` i ON e.user_id = i.user_id
        AND DATE(e.event_time) = DATE(i.event_time)
    WHERE
        DATE(e.event_time) >= "2022-01-01"
),
raw1 as (
    SELECT
        DISTINCT user_id ,
        -- session_id,
        event_time,
        event_date,
        event_name,
        event_parameter.key AS e_key,
        event_parameter.value.string_value AS e_str_val,
        COALESCE(
            event_parameter.value.float_value,
            event_parameter.value.int_value
        ) AS value,
        device_os,
        country
    FROM
        raw e,
        unnest (e.event_parameter) AS event_parameter
    WHERE
        DATE(event_time) >= "2022-01-01"
),
user_info AS (
    SELECT
        DISTINCT user_id,
        event_date,
        device_os,
        country
    FROM
        raw1
),
session_start AS (
    SELECT
        DISTINCT user_id,
        event_time,
        event_date ,
        -- event_name,
        user_id || '-' || ROW_NUMBER() OVER(
            PARTITION BY user_id
            ORDER BY
                event_time
        ) || '-' || event_date AS session_id,
        device_os,
        country
    FROM
        raw
    WHERE
        true
        AND event_name IN ('session_start')
),
sessions2 AS (
    SELECT
        *,
        event_time AS session_start_at,
        LEAD(event_time) OVER(
            PARTITION BY user_id
            ORDER BY
                event_time
        ) AS next_session_start_at
    FROM
        session_start
),
session3 AS (
    SELECT
        s.user_id,
        s.session_id,
        r.event_name,
        r.event_time
    FROM
        sessions2 s
        LEFT JOIN raw1 r ON r.user_id = s.user_id
        AND s.session_start_at <= r.event_time
        AND (
            r.event_time < s.next_session_start_at
            OR next_session_start_at IS NULL
        ) -- less but not equal
),
session_durationtb AS (
    SELECT
        user_id,
        event_date,
        sum(duration) AS session_duration
    FROM
        (
            SELECT
                s.user_id,
                s.session_id,
                DATE(event_time) event_date,
                TIMESTAMP_DIFF(MAX(s.event_time), MIN(s.event_time), MINUTE) AS duration
            FROM
                session3 s
            GROUP BY
                1,
                2,
                3
            ORDER BY
                1 DESC
        )
    GROUP BY
        1,
        2
),
gameplay AS (
    SELECT
        user_id,
        event_time,
        event_date,
        event_name
    FROM
        raw
    WHERE
        true
        AND event_name IN ('gameplay_start')
),
loading AS (
    SELECT
        user_id,
        event_date,
        COUNT(
            DISTINCT CASE WHEN event_name = 'loading_start' THEN user_id END
        ) loading_start,
        COUNT(
            DISTINCT CASE WHEN event_name = 'loading_completed' THEN user_id END
        ) loading_completed,
        COUNT(
            DISTINCT CASE WHEN event_name = 'loading_failed' THEN user_id END
        ) loading_failed
    FROM
        (
            SELECT
                DISTINCT user_id,
                event_time,
                event_date,
                event_name
            FROM
                raw
            WHERE
                true
                AND event_name IN (
                    'loading_start',
                    'loading_completed',
                    'loading_failed'
                )
        )
    GROUP BY
        1,
        2
),
first_active AS (
    SELECT
        user_id,
        MIN(event_time) first_active_time
    FROM
        session_start
    GROUP BY
        1
),
raw_table AS (
    SELECT
        *,
        CASE WHEN g_user_id IS NOT NULL THEN s.user_id END AS gameplay
    FROM
        (
            SELECT
                DISTINCT s.*
            EXCEPT(event_time),
                f.first_active_time,
                g.user_id AS g_user_id
            FROM
                session_start s
                LEFT JOIN first_active f ON s.user_id = f.user_id
                AND DATE(s.event_date) = DATE(f.first_active_time)
                LEFT JOIN gameplay g ON s.user_id = g.user_id
                AND s.event_date = g.event_date --and s.event_time <= g.event_time
            ORDER BY
                1,
                3
        ) AS s
),
final AS (
    SELECT
        distinct r.event_date,(r.user_id) AS dau,(gameplay) AS gameplay,
        first_active_time,
        s.session_duration,
        l.loading_start,
        loading_completed,
        loading_failed,
        device_os,
        country
    FROM
        raw_table r
        LEFT JOIN session_durationtb s ON r.user_id = s.user_id
        AND r.event_date = s.event_date
        LEFT JOIN loading l ON r.user_id = l.user_id
        AND r.event_date = l.event_date
)
SELECT
    *
FROM
    final