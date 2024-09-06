{{- config(
    materialized='table',
)-}}

WITH remove_duplicate AS
(	
	SELECT *EXCEPT(rk)
	FROM (
		SELECT *,
			ROW_NUMBER() OVER(PARTITION BY user_id, session_id ORDER BY gameplay_start_event_timestamp) rk,
		FROM {{ ref('fct_level_design_gameplay') }}
	)
	WHERE rk = 1
)

,max_lvl_reach AS
(
	SELECT  
		user_id
		,session_id
		,MAX(level_start_level_count) level_start_level_count
		,SUM(time_played)time_played
	FROM (
		SELECT DISTINCT 
			user_id
	       ,session_id
	       ,level_start_level_count
		   ,time_played
		FROM  {{ ref('mart_level_design_lvl') }}
	)
	WHERE TRUE
	GROUP BY  1,2
) 

, raw AS
(
	SELECT DISTINCT 
		a.* EXCEPT(gameplay_level_count,gameplay_time_played)
	    ,COALESCE(gameplay_level_count,level_start_level_count )gameplay_level_count
		,COALESCE(gameplay_time_played,time_played )gameplay_time_played
		,CAST(character_PS AS INT64) + CAST(armor_PS AS INT64) + CAST(head_PS AS INT64) + CAST(shoes_PS AS INT64)+ CAST(gloves_PS AS INT64) + CAST(weapon1_PS AS INT64) + CAST(weapon2_PS AS INT64) + CAST(legs_PS AS INT64) AS totalPS
		,UPPER(CONCAT(
					CASE 
						WHEN a.difficulty  LIKE CONCAT('%',dim_difficulty.original_name,'%') THEN dim_difficulty.rename
						ELSE a.difficulty
					END 
					,'_'
					,
					REPLACE(
							CASE  
								WHEN a.dungeon_id LIKE '%ENDLESS%' THEN 'ENDLESS'
								WHEN a.dungeon_id LIKE '%SHOOTEMUP%' THEN 'STU'
								WHEN a.dungeon_id = 'DUNGEON_DOPAMIS' THEN '01DP_01'
								WHEN a.dungeon_id LIKE CONCAT('%',dim_dungeon.original_name,'%') THEN REPLACE(a.dungeon_id, dim_dungeon.original_name, dim_dungeon.rename)
								ELSE a.dungeon_id
							END
						, 'DUNGEON_', '')

		))
		 AS dungeon_id_difficulty

	FROM remove_duplicate a
	LEFT JOIN max_lvl_reach ml
	ON a.user_id = ml.user_id AND a.user_id = ml.user_id AND a.session_id = ml.session_id
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_dungeon
	ON a.dungeon_id  LIKE CONCAT('%',dim_dungeon.original_name,'%')
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_difficulty
	ON a.difficulty  LIKE CONCAT('%',dim_difficulty.original_name,'%')
) 

, max_min_ps_date_diff AS
(
	SELECT DISTINCT 
		user_id
		,day_diff
		,MAX(totalPS) maxtotalPS
		,MAX(gameplay_start_event_timestamp)max_gameplay_start_event_timestamp
		,MIN(totalPS) min_totalPS
		,MIN(gameplay_start_event_timestamp)min_gameplay_start_event_timestamp
	FROM raw
	GROUP BY 1,2
)

,final AS 
(
	SELECT DISTINCT 
		raw.*
       ,m.maxtotalPS
	   ,min_.min_totalPS
       ,DENSE_RANK() OVER (PARTITION BY raw.user_id,mode,difficulty ,dungeon_id ORDER BY gameplay_start_event_timestamp)                 AS dungeon_start_cnt
       ,DENSE_RANK() OVER (PARTITION BY raw.user_id,mode,difficulty ,dungeon_id,gameplay_status ORDER BY gameplay_start_event_timestamp) AS dungeon_win_cnt

	FROM raw
	LEFT JOIN max_min_ps_date_diff m ON raw.user_id = m.user_id AND raw.day_diff = m.day_diff AND raw.gameplay_start_event_timestamp = m.max_gameplay_start_event_timestamp
	LEFT JOIN max_min_ps_date_diff min_ ON raw.user_id = min_.user_id AND raw.day_diff = min_.day_diff AND raw.gameplay_start_event_timestamp = min_.min_gameplay_start_event_timestamp
)

SELECT DISTINCT 
	f.*,
	COALESCE(media_source,'unknown') AS media_source,
FROM final f
LEFT JOIN (SELECT DISTINCT customer_user_id, media_source
              FROM `sipherg1production.raw_appsflyer_locker.installs` 
              WHERE TIMESTAMP_TRUNC(_dl_partition_time, DAY) > TIMESTAMP("2024-05-27") 
              AND media_source = 'hangmyads_int') AS af ON f.user_id = af.customer_user_id
