{{- config(
    materialized='table',
)-}}

WITH remove_duplicate AS
(	
	SELECT *EXCEPT(rk)
	FROM (
		SELECT *,
			ROW_NUMBER() OVER(PARTITION BY user_id, session_id, level_start_level_count ORDER BY level_start_event_timestamp) rk,
		FROM {{ ref('fct_level_design_lvl') }}
	)
	WHERE rk = 1
)

,raw AS
(
	SELECT DISTINCT 
		a.*
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
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_dungeon
	ON a.dungeon_id  LIKE CONCAT('%',dim_dungeon.original_name,'%')
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_difficulty
	ON a.difficulty  LIKE CONCAT('%',dim_difficulty.original_name,'%')
	WHERE true 
) 

, rank_ AS
(
	SELECT  
		*
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id ORDER BY gameplay_start_event_timestamp) AS dungeon_start_cnt
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id, gameplay_status ORDER BY gameplay_start_event_timestamp) AS dungeon_win_cnt
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id,level_start_level_count ORDER BY level_start_event_timestamp) AS dungeon_lvl_start_cnt
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id,level_start_level_count,level_status ORDER BY level_start_event_timestamp) AS dungeon_lvl_win_cnt
		--timeplay của level là accumulated 
		,LAG(time_played) OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id, session_id ORDER BY level_start_event_timestamp) AS lag_time_played

	FROM raw
)
 
,final AS 
(
	SELECT  
		f.*except(time_played,lag_time_played ),
		f.time_played as cumulative_time_played,
		COALESCE( f.time_played - lag_time_played,f.time_played)  as time_played,
	FROM rank_ f
)

SELECT DISTINCT 
	f.*,
	COALESCE(media_source,'unknown') AS media_source,
FROM final f
LEFT JOIN (SELECT DISTINCT customer_user_id, media_source
              FROM `sipherg1production.raw_appsflyer_locker.installs` 
              WHERE TIMESTAMP_TRUNC(_dl_partition_time, DAY) > TIMESTAMP("2024-05-27") 
              AND media_source = 'hangmyads_int') AS af ON f.user_id = af.customer_user_id

