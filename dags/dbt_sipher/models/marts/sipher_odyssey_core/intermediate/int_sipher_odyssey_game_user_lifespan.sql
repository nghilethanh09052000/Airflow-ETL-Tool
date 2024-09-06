{{- config(
  materialized='table'
) -}}

WITH raw AS (
  SELECT *
	FROM {{ ref('int_sipher_odyssey_login') }}
  WHERE ather_id <> 'unknown'
)

,get_duration_of_game_user_id AS (
  SELECT
    DISTINCT
    ather_id,
    user_id AS game_user_id,
    MIN(event_timestamp) AS start_timestamp,
    MAX(event_timestamp) AS end_timestamp,
  FROM raw
  GROUP BY ALL
)

,rktb AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY ather_id ORDER BY start_timestamp DESC) AS ather_rk
  FROM get_duration_of_game_user_id
)

SELECT
  DISTINCT
  ather_id,
  game_user_id,
  start_timestamp,
  CASE
    WHEN ather_rk = 1 THEN UNIX_MICROS('2026-01-01')
    ELSE end_timestamp
  END AS end_timestamp,
  CASE
    WHEN ather_rk = 1 THEN 1
    ELSE 0
  END AS is_active,
FROM rktb