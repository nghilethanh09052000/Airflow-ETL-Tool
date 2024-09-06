{{- config(
    materialized='table',
)-}}

WITH raw AS
(
	SELECT DISTINCT 
        user_id
        ,day0_date_tzutc
        ,email
        ,dungeon_id_difficulty
        ,timestamp_micros(gameplay_start_event_timestamp) gameplay_start_event_timestamp
        ,gameplay_time_played
        ,gameplay_status
        ,DENSE_RANK() OVER (PARTITION BY user_id,dungeon_id_difficulty ORDER BY gameplay_start_event_timestamp) AS dungeon_start_cnt
	FROM {{ ref('mart_level_design_gameplay') }}
	WHERE true 
) 

, dungeon_win  AS
(
	SELECT DISTINCT 
        user_id
        ,dungeon_id_difficulty
        ,dungeon_start_cnt
        ,gameplay_start_event_timestamp,day0_date_tzutc
        ,DENSE_RANK() OVER (PARTITION BY user_id,dungeon_id_difficulty ORDER BY gameplay_start_event_timestamp) AS dungeon_win_cnt
	FROM raw
	WHERE gameplay_status = 'SUCCESS' 
) 

, sum_time_played_to_wintb AS
(
	SELECT 
        a.user_id
        ,a.dungeon_id_difficulty
        ,SUM(b.gameplay_time_played) AS sum_time_played_to_win
	FROM dungeon_win a
	LEFT JOIN raw b
	    ON a.user_id = b.user_id AND a.dungeon_win_cnt = 1 AND a.dungeon_start_cnt >= b.dungeon_start_cnt AND a.dungeon_id_difficulty = b.dungeon_id_difficulty
	WHERE a.dungeon_win_cnt = 1
	GROUP BY 1,2
)

,attempts_to_wintb AS 
(
	SELECT
        user_id
        ,dungeon_id_difficulty
        ,dungeon_start_cnt
 	FROM dungeon_win
	WHERE dungeon_win_cnt = 1
)

,day_diff_win AS 
(
	SELECT
        a.user_id
        ,a.dungeon_id_difficulty
        ,DATE_DIFF(DATE(gameplay_start_event_timestamp ), day0_date_tzutc, DAY) AS day_diff
 	FROM dungeon_win a
 	WHERE a.dungeon_win_cnt = 1
 
)

, ordered_dungeon AS
(
	SELECT
        user_id
        ,day0_date_tzutc
        ,email
        ,dungeon_id_difficulty
	FROM raw
	WHERE dungeon_start_cnt = 1 
)

SELECT DISTINCT
    a.*
    ,c.sum_time_played_to_win
    ,c1.dungeon_start_cnt as attempts_to_win
    ,d.day_diff
	,COALESCE(media_source,'unknown') AS media_source,
FROM ordered_dungeon a
LEFT JOIN sum_time_played_to_wintb c
    ON c.user_id = a.user_id AND c.dungeon_id_difficulty = a.dungeon_id_difficulty
LEFT JOIN attempts_to_wintb c1
    ON c1.user_id = a.user_id AND c1.dungeon_id_difficulty = a.dungeon_id_difficulty
LEFT JOIN day_diff_win d
    ON d.user_id = a.user_id AND d.dungeon_id_difficulty = a.dungeon_id_difficulty
LEFT JOIN (SELECT DISTINCT customer_user_id, media_source
              FROM `sipherg1production.raw_appsflyer_locker.installs` 
              WHERE TIMESTAMP_TRUNC(_dl_partition_time, DAY) > TIMESTAMP("2024-05-27") 
              AND media_source = 'hangmyads_int') AS af ON a.user_id = af.customer_user_id