{{- config(
    materialized='incremental',
	merge_update_columns = [
    'user_id',
    'session_id'
  ],
	partition_by={
		'field': 'day0_date_tzutc',
		'data_type': 'DATE',
	},
	cluster_by=['day_diff']
  )-}}

WITH raw AS
(
	SELECT  *
	FROM {{ ref('stg_firebase__sipher_odyssey_events_14d') }}
) 

, cohort_user AS
(
	SELECT DISTINCT 
		ather_id
		,is_atherlabs_employee
		,email
		,user_id
		,user_name
		,country
		,MIN(day0_date_tzutc) AS day0_date_tzutc
	FROM {{ ref('dim_sipher_odyssey_player') }}
	GROUP BY ALL
) 

, login_start_raw AS
(
	SELECT DISTINCT 
		user_id
		,event_name
		,{{ get_string_value_from_event_params(key="build_number") }} AS build_number 
		,app_info.version AS app_version
    	,device.operating_system AS operating_system
		,MIN (event_timestamp) AS current_build_timestamp
	FROM raw
	WHERE event_name = 'login_start'
	GROUP BY  user_id
	         ,event_name
	         ,build_number
	         ,app_version
			 ,operating_system
) 

, login_start AS
(
	SELECT DISTINCT 
		login_start_raw.user_id
		,ather_id
		,cohort.day0_date_tzutc                                                                                       AS day0_date_tzutc
		,event_name
		,build_number
		,app_version
		,country
		,operating_system
		,email
		,is_atherlabs_employee
		,user_name
		,current_build_timestamp
		,LEAD(current_build_timestamp,1) OVER (PARTITION BY login_start_raw.user_id ORDER BY current_build_timestamp) AS next_build_timestamp
	FROM login_start_raw
	JOIN cohort_user cohort
	ON (login_start_raw.user_id = cohort.user_id)
) 

, gameplay_start AS
(
	SELECT DISTINCT 
		event_date
		,event_timestamp
		,COALESCE(raw.user_pseudo_id)                                          AS user_pseudo_id
		,raw.user_id                                                           AS user_id
		,cohort.day0_date_tzutc                                                AS day0_date_tzutc
		,date_diff(PARSE_DATE('%Y%m%d',event_date),cohort.day0_date_tzutc,day) AS day_diff
		,event_name
		,{{ get_string_value_from_event_params(key="session_id") }} AS session_id
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.dungeon_id' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.dungeon_id' )
			)) AS dungeon_id
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.room_id' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.room_id' )
			)) AS room_id
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.party_code' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.party_code' )
			)) AS party_code
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.team' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.team' )
			)) AS team
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.spectator' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.spectator' )
			)) AS spectator
		,UPPER({{ get_string_value_from_event_params(key="mode") }}) AS mode
		,UPPER({{ get_string_value_from_event_params(key="difficulty") }}) AS difficulty
		,UPPER({{ get_string_value_from_event_params(key="character_instance_id") }}) AS character_instance_id
		,UPPER({{ get_string_value_from_event_params(key="energy_ticket") }}) AS energy_ticket
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_1") }}) AS corruption_pool_1
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_2") }}) AS corruption_pool_2
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_3") }}) AS corruption_pool_3
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_4") }}) AS corruption_pool_4
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_5") }}) AS corruption_pool_5
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_6") }}) AS corruption_pool_6
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_7") }}) AS corruption_pool_7
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.race' )) AS race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.sub_race' )) AS sub_race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.level' )) AS character_level
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.PS' )) AS character_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="armor")}}, '$.id' )) AS armor
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="armor")}}, '$.PS' )) AS armor_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="head")}}, '$.id' )) AS head
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="head")}}, '$.PS' )) AS head_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="shoes")}}, '$.id' )) AS shoes
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="shoes")}}, '$.PS' )) AS shoes_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="legs")}}, '$.id' )) AS legs
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="legs")}}, '$.PS' )) AS legs_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gloves")}}, '$.id' )) AS gloves
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gloves")}}, '$.PS' )) AS gloves_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_1")}}, '$.id' )) AS weapon1
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_1")}}, '$.PS' )) AS weapon1_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_2")}}, '$.id' )) AS weapon2
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_2")}}, '$.PS' )) AS weapon2_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="boosters")}}, '$.xp' )) AS booster_id
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="boosters")}}, '$.drop' )) AS booster_drop
	FROM raw , UNNEST
	(event_params
	) AS ep
	JOIN cohort_user cohort
	ON raw.user_id = cohort.user_id
	WHERE event_name IN ('gameplay_start')
	 
)

, gameplay_start1 AS
(
	SELECT DISTINCT 
		COALESCE(raw.user_pseudo_id)                                          AS user_pseudo_id
		,{{ get_string_value_from_event_params(key="session_id") }} AS session_id
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.dungeon_id' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.dungeon_id' )
			)) AS dungeon_id
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.room_id' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.room_id' )
			)) AS room_id
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.party_code' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.party_code' )
			)) AS party_code
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.team' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.team' )
			)) AS team
		,UPPER(COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.spectator' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info_1")}}, '$.spectator' )
			)) AS spectator
		,UPPER({{ get_string_value_from_event_params(key="mode") }}) AS mode
		,UPPER({{ get_string_value_from_event_params(key="difficulty") }}) AS difficulty
		,UPPER({{ get_string_value_from_event_params(key="character_instance_id") }}) AS character_instance_id
		,UPPER({{ get_string_value_from_event_params(key="energy_ticket") }}) AS energy_ticket
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_1") }}) AS corruption_pool_1
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_2") }}) AS corruption_pool_2
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_3") }}) AS corruption_pool_3
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_4") }}) AS corruption_pool_4
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_5") }}) AS corruption_pool_5
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_6") }}) AS corruption_pool_6
		,UPPER({{ get_string_value_from_event_params(key="corruption_pool_7") }}) AS corruption_pool_7
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.race' )) AS race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.sub_race' )) AS sub_race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.level' )) AS character_level
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.PS' )) AS character_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="armor")}}, '$.id' )) AS armor
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="armor")}}, '$.PS' )) AS armor_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="head")}}, '$.id' )) AS head
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="head")}}, '$.PS' )) AS head_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="shoes")}}, '$.id' )) AS shoes
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="shoes")}}, '$.PS' )) AS shoes_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="legs")}}, '$.id' )) AS legs
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="legs")}}, '$.PS' )) AS legs_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gloves")}}, '$.id' )) AS gloves
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gloves")}}, '$.PS' )) AS gloves_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_1")}}, '$.id' )) AS weapon1
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_1")}}, '$.PS' )) AS weapon1_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_2")}}, '$.id' )) AS weapon2
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_2")}}, '$.PS' )) AS weapon2_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="boosters")}}, '$.xp' )) AS booster_id
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="boosters")}}, '$.drop' )) AS booster_drop
	FROM raw , UNNEST
	(event_params
	) AS ep
	WHERE event_name IN ('gameplay_start_1')
)

, gameplay_end AS(
	SELECT  DISTINCT 
		event_date
		,event_timestamp
		,COALESCE(raw.user_pseudo_id)                                          AS user_pseudo_id
		,raw.user_id                                                           AS user_id
		,cohort.day0_date_tzutc                                                AS day0_date_tzutc
		,date_diff(PARSE_DATE('%Y%m%d',event_date),cohort.day0_date_tzutc,day) AS day_diff
		,event_name
		,{{ get_string_value_from_event_params(key="session_id") }} AS session_id
		,UPPER({{ get_string_value_from_event_params(key="mode") }}) AS mode
		,{{ get_double_value_from_event_params(key="player_amount") }} AS player_amount
		,{{ get_double_value_from_event_params(key="player_remain") }} AS player_remain
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_1") }}) AS activated_corruptions_1
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_2") }}) AS activated_corruptions_2
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_3") }}) AS activated_corruptions_3
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_4") }}) AS activated_corruptions_4
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_5") }}) AS activated_corruptions_5
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_6") }}) AS activated_corruptions_6
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_7") }}) AS activated_corruptions_7
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.race' )) AS race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.sub_race' )) AS sub_race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.level' )) AS character_level
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.PS' )) AS character_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.status' )) AS status
		,SPLIT(UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.level_count' )), '/')[OFFSET(0)] AS level_count
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.time_played' )) AS time_played


		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.get_hit' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.get_hit' )
		)) AS action_count_get_hit
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.skill' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.skill' )
		)) AS action_count_skill
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.dash' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.dash' )
		)) AS action_count_dash
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.changeweapon' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.changeweapon' )
		)) AS action_count_changeweapon
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.isurge_range' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.isurge_range' )
		)) AS action_count_isurge_range
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.isurge_melee' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.isurge_melee' )
		)) AS action_count_isurge_melee
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.skill' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.skill' )
		)) AS damage_dealt_skill
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.ranged' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.ranged' )
		)) AS damage_dealt_ranged
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.meele' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.meele' )
		)) AS damage_dealt_meele
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.status_effect' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.status_effect' )
		)) AS damage_dealt_status_effect
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.isurge_range' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.isurge_range' )
		)) AS damage_dealt_isurge_range
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.isurge_melee' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.isurge_melee' )
		)) AS damage_dealt_isurge_melee
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.enemy' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.enemy' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.enemy' )
		)) AS enemy
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.down' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.down' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.down' )
		)) AS down
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.revive' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.revive' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.revive' )
		)) AS revive
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.being_revived' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.being_revived' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.being_revived' )
		)) AS being_revived
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.hp_loss' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.hp_loss' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.hp_loss' )
		)) AS hp_loss
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.hp_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.hp_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.hp_pct' )
		)) AS hp_pct
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.killed_by_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.killed_by_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.killed_by_enemy_player' )
		)) AS killed_by_enemy_player
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.healing_pad_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.healing_pad_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.healing_pad_pct' )
		)) AS healing_pad_pct
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.healing_pad' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.healing_pad' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.healing_pad' )
		)) AS healing_pad
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.score' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.score' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.score' )
		)) AS score
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.killed_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.killed_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.killed_enemy_player' )
		)) AS killed_enemy_player
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.deposit_point_amount' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.deposit_point_amount' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.deposit_point_amount' )
		)) AS deposit_point_amount
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.deposit_point_freq' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.deposit_point_freq' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.deposit_point_freq' )
		)) AS deposit_point_freq
	FROM raw
	JOIN cohort_user cohort
	ON raw.user_id = cohort.user_id
	WHERE event_name IN ('gameplay_end')
	
)


, gameplay_end1 AS(
	SELECT  DISTINCT 
		COALESCE(raw.user_pseudo_id)                                          AS user_pseudo_id
		,{{ get_string_value_from_event_params(key="session_id") }} AS session_id
		,UPPER({{ get_string_value_from_event_params(key="mode") }}) AS mode
		,{{ get_double_value_from_event_params(key="player_amount") }} AS player_amount
		,{{ get_double_value_from_event_params(key="player_remain") }} AS player_remain
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_1") }}) AS activated_corruptions_1
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_2") }}) AS activated_corruptions_2
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_3") }}) AS activated_corruptions_3
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_4") }}) AS activated_corruptions_4
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_5") }}) AS activated_corruptions_5
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_6") }}) AS activated_corruptions_6
		,UPPER({{ get_string_value_from_event_params(key="activated_corruptions_7") }}) AS activated_corruptions_7
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.race' )) AS race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.sub_race' )) AS sub_race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.level' )) AS character_level
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.PS' )) AS character_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.status' )) AS status
		,SPLIT(UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.level_count' )), '/')[OFFSET(0)] AS level_count
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.time_played' )) AS time_played
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.get_hit' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.get_hit' )
		)) AS action_count_get_hit
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.skill' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.skill' )
		)) AS action_count_skill
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.dash' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.dash' )
		)) AS action_count_dash
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.changeweapon' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.changeweapon' )
		)) AS action_count_changeweapon
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.isurge_range' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.isurge_range' )
		)) AS action_count_isurge_range
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}} , '$.isurge_melee' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count_1")}}, '$.isurge_melee' )
		)) AS action_count_isurge_melee
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.skill' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.skill' )
		)) AS damage_dealt_skill
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.ranged' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.ranged' )
		)) AS damage_dealt_ranged
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.meele' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.meele' )
		)) AS damage_dealt_meele
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.status_effect' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.status_effect' )
		)) AS damage_dealt_status_effect
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.isurge_range' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.isurge_range' )
		)) AS damage_dealt_isurge_range
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}} , '$.isurge_melee' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt_1")}}, '$.isurge_melee' )
		)) AS damage_dealt_isurge_melee
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.enemy' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.enemy' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.enemy' )
		)) AS enemy
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.down' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.down' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.down' )
		)) AS down
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.revive' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.revive' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.revive' )
		)) AS revive
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.being_revived' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.being_revived' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.being_revived' )
		)) AS being_revived
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.hp_loss' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.hp_loss' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.hp_loss' )
		)) AS hp_loss
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.hp_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.hp_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.hp_pct' )
		)) AS hp_pct
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.killed_by_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.killed_by_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.killed_by_enemy_player' )
		)) AS killed_by_enemy_player
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.healing_pad_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.healing_pad_pct' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.healing_pad_pct' )
		)) AS healing_pad_pct
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.healing_pad' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.healing_pad' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.healing_pad' )
		)) AS healing_pad
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.score' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.score' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.score' )
		)) AS score
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.killed_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.killed_enemy_player' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.killed_enemy_player' )
		)) AS killed_enemy_player
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.deposit_point_amount' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.deposit_point_amount' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.deposit_point_amount' )
		)) AS deposit_point_amount
		,UPPER(
		COALESCE(
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}} , '$.deposit_point_freq' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_1")}} , '$.deposit_point_freq' ),
			JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count_2")}}, '$.deposit_point_freq' )
		)) AS deposit_point_freq
	FROM raw
	WHERE event_name IN ('gameplay_end_1')
)

, gameplay_final AS
(
	SELECT  DISTINCT 
		gameplay_start.event_date                                                   		  AS gameplay_start_event_date
		,gameplay_start.event_timestamp                                                       AS gameplay_start_event_timestamp
		,login_start.build_number                                                             AS build_number
		,login_start.app_version															  AS app_version
		,login_start.country														  		  AS country
		,login_start.operating_system														  AS operating_system
		,login_start.email																	  AS email
		,login_start.is_atherlabs_employee													  AS is_atherlabs_employee
		,user_name																			  AS user_name
		,gameplay_start.user_pseudo_id                                                        AS user_pseudo_id
		,COALESCE(gameplay_start.user_id,gameplay_end.user_id)                                AS user_id
		,COALESCE(gameplay_start.day0_date_tzutc,gameplay_end.day0_date_tzutc)                AS day0_date_tzutc
		,COALESCE(gameplay_start.day_diff,gameplay_end.day_diff)                              AS day_diff
		,gameplay_start.event_name                                                            AS gameplay_start_event_name
		,COALESCE(gameplay_start.session_id, gameplay_start1.session_id)                      AS session_id
		,COALESCE(gameplay_start.dungeon_id, gameplay_start1.dungeon_id)                      AS dungeon_id
		,COALESCE(gameplay_start.room_id, gameplay_start1.room_id)                            AS room_id
		,COALESCE(gameplay_start.party_code, gameplay_start1.party_code)                      AS party_code
		,COALESCE(gameplay_start.team, gameplay_start1.team)                                  AS team
		,COALESCE(gameplay_start.spectator, gameplay_start1.spectator)                        AS spectator
		,COALESCE(gameplay_start.mode, gameplay_start1.mode, gameplay_end.mode, gameplay_end1.mode) AS mode
		,COALESCE(gameplay_start.difficulty, gameplay_start1.difficulty)                        AS difficulty
		,COALESCE(gameplay_start.character_instance_id, gameplay_start1.character_instance_id)  AS character_instance_id
		,CAST(CASE WHEN COALESCE(gameplay_start.energy_ticket, gameplay_start1.energy_ticket) = 'NULL' THEN NULL ELSE COALESCE(gameplay_start.energy_ticket, gameplay_start1.energy_ticket) END AS INT64)                                                            AS energy_ticket
		,COALESCE(gameplay_start.corruption_pool_1, gameplay_start1.corruption_pool_1)  		AS corruption_pool_1
		,COALESCE(gameplay_start.corruption_pool_2, gameplay_start1.corruption_pool_2)  		AS corruption_pool_2
		,COALESCE(gameplay_start.corruption_pool_3, gameplay_start1.corruption_pool_3)  		AS corruption_pool_3
		,COALESCE(gameplay_start.corruption_pool_4, gameplay_start1.corruption_pool_4)  		AS corruption_pool_4
		,COALESCE(gameplay_start.corruption_pool_5, gameplay_start1.corruption_pool_5)  		AS corruption_pool_5
		,COALESCE(gameplay_start.corruption_pool_6, gameplay_start1.corruption_pool_6)  		AS corruption_pool_6
		,COALESCE(gameplay_start.corruption_pool_7, gameplay_start1.corruption_pool_7)  		AS corruption_pool_7
		,COALESCE(gameplay_start.race, gameplay_start1.race, gameplay_end.race, gameplay_end1.race) AS race
		,COALESCE(gameplay_start.sub_race, gameplay_start1.sub_race, gameplay_end.sub_race, gameplay_end1.sub_race) AS sub_race
		,CAST(COALESCE(gameplay_start.character_level, gameplay_start1.character_level, gameplay_end.character_level, gameplay_end1.character_level) AS INT64) AS character_level
		--character_PS trong event dang la sum ps
		,CAST(COALESCE(gameplay_start.character_PS,gameplay_end.character_PS) AS INT64) - CAST(gameplay_start.armor_PS AS INT64) - CAST(gameplay_start.head_PS AS INT64) - CAST(gameplay_start.shoes_PS AS INT64) - CAST(gameplay_start.legs_PS AS INT64) - CAST(gameplay_start.gloves_PS AS INT64) - CAST(gameplay_start.weapon1_PS AS INT64) - CAST(gameplay_start.weapon2_PS AS INT64) AS character_PS
		,COALESCE(gameplay_start.armor, gameplay_start1.armor)  							  AS armor
		,CAST(COALESCE(gameplay_start.armor_PS, gameplay_start1.armor_PS) AS INT64)           AS armor_PS
		,COALESCE(gameplay_start.head, gameplay_start1.head)  							      AS head
		,CAST(COALESCE(gameplay_start.head_PS, gameplay_start1.head_PS)  AS INT64)            AS head_PS
		,COALESCE(gameplay_start.shoes, gameplay_start1.shoes)  							  AS shoes
		,CAST(COALESCE(gameplay_start.shoes_PS, gameplay_start1.shoes_PS) AS INT64)           AS shoes_PS
		,COALESCE(gameplay_start.legs, gameplay_start1.legs)  							      AS legs
		,CAST(COALESCE(gameplay_start.legs_PS, gameplay_start1.legs_PS) AS INT64)             AS legs_PS
		,COALESCE(gameplay_start.gloves, gameplay_start1.gloves)  							  AS gloves
		,CAST(COALESCE(gameplay_start.gloves_PS, gameplay_start1.gloves_PS) AS INT64)         AS gloves_PS
		,COALESCE(gameplay_start.weapon1, gameplay_start1.weapon1)  						  AS weapon1
		,CAST(COALESCE(gameplay_start.weapon1_PS, gameplay_start1.weapon1_PS) AS INT64)       AS weapon1_PS
		,COALESCE(gameplay_start.weapon2, gameplay_start1.weapon2)  						  AS weapon2
		,CAST(COALESCE(gameplay_start.weapon2_PS, gameplay_start1.weapon2_PS) AS INT64)       AS weapon2_PS
		,COALESCE(gameplay_start.booster_id, gameplay_start1.booster_id)  					  AS booster_id
		,COALESCE(gameplay_start.booster_drop, gameplay_start1.booster_drop)  				  AS booster_drop
		,gameplay_end.event_date                                                              AS gameplay_end_event_date
		,gameplay_end.event_timestamp                                                         AS gameplay_end_event_timestamp
		,gameplay_end.event_name                                                              AS gameplay_end_event_name
		,COALESCE(gameplay_end.activated_corruptions_1, gameplay_end1.activated_corruptions_1)  AS gameplay_end_activated_corruptions_1
		,COALESCE(gameplay_end.activated_corruptions_2, gameplay_end1.activated_corruptions_2)  AS gameplay_end_activated_corruptions_2
		,COALESCE(gameplay_end.activated_corruptions_3, gameplay_end1.activated_corruptions_3)  AS gameplay_end_activated_corruptions_3
		,COALESCE(gameplay_end.activated_corruptions_4, gameplay_end1.activated_corruptions_4)  AS gameplay_end_activated_corruptions_4
		,COALESCE(gameplay_end.activated_corruptions_5, gameplay_end1.activated_corruptions_5)  AS gameplay_end_activated_corruptions_5
		,COALESCE(gameplay_end.activated_corruptions_6, gameplay_end1.activated_corruptions_6)  AS gameplay_end_activated_corruptions_6
		,COALESCE(gameplay_end.activated_corruptions_7, gameplay_end1.activated_corruptions_7)  AS gameplay_end_activated_corruptions_7
		,CAST(COALESCE(gameplay_end.player_amount, gameplay_end1.player_amount) AS INT64)       AS gameplay_player_amount
		,CAST(COALESCE(gameplay_end.player_remain, gameplay_end1.player_remain) AS INT64)       AS gameplay_player_remain
		,COALESCE(gameplay_end.status, gameplay_end1.status,'UNDETECTED')                       AS gameplay_status
		,CAST(COALESCE(NULLIF(COALESCE(gameplay_end.level_count, gameplay_end1.level_count),''),NULL) AS INT64) AS gameplay_level_count 
		,CAST(COALESCE(gameplay_end.time_played, gameplay_end1.time_played) AS INT64)                           AS gameplay_time_played
		,CAST(COALESCE(gameplay_end.action_count_get_hit, gameplay_end1.action_count_get_hit) AS FLOAT64)       AS gameplay_action_count_get_hit
		,CAST(COALESCE(gameplay_end.action_count_skill, gameplay_end1.action_count_skill) AS INT64)             AS gameplay_action_count_skill
		,CAST(COALESCE(gameplay_end.action_count_dash, gameplay_end1.action_count_dash) AS INT64)               AS gameplay_action_count_dash
		,CAST(COALESCE(gameplay_end.action_count_changeweapon, gameplay_end1.action_count_changeweapon) AS INT64) AS gameplay_action_count_changeweapon
		,CAST(COALESCE(gameplay_end.damage_dealt_skill, gameplay_end1.damage_dealt_skill) AS INT64)               AS gameplay_damage_dealt_skill
		,CAST(COALESCE(gameplay_end.damage_dealt_ranged, gameplay_end1.damage_dealt_ranged) AS INT64)             AS gameplay_damage_dealt_ranged
		,CAST(COALESCE(gameplay_end.damage_dealt_meele, gameplay_end1.damage_dealt_meele) AS INT64)               AS gameplay_damage_dealt_meele
		,COALESCE(gameplay_end.damage_dealt_status_effect, gameplay_end1.damage_dealt_status_effect)              AS gameplay_damage_dealt_status_effect
		,COALESCE(gameplay_end.damage_dealt_isurge_range, gameplay_end1.damage_dealt_isurge_range)                AS gameplay_damage_dealt_isurge_range
		,COALESCE(gameplay_end.damage_dealt_isurge_melee, gameplay_end1.damage_dealt_isurge_melee)                AS gameplay_damage_dealt_isurge_melee
		,CAST(COALESCE(gameplay_end.enemy, gameplay_end1.enemy) AS INT64)                                         AS gameplay_enemy
		,CAST(COALESCE(gameplay_end.down, gameplay_end1.down) AS INT64)                                           AS gameplay_down
		,CAST(COALESCE(gameplay_end.revive, gameplay_end1.revive) AS INT64)                                       AS gameplay_revive
		,CAST(COALESCE(gameplay_end.being_revived, gameplay_end1.being_revived) AS INT64)                         AS gameplay_being_revived
		,CAST(COALESCE(gameplay_end.hp_loss, gameplay_end1.hp_loss) AS FLOAT64)                                   AS gameplay_hp_loss
		,CAST(COALESCE(gameplay_end.hp_pct, gameplay_end1.hp_pct) AS FLOAT64)                                     AS gameplay_hp_pct
		,CAST(COALESCE(gameplay_end.killed_by_enemy_player, gameplay_end1.killed_by_enemy_player) AS FLOAT64)     AS gameplay_killed_by_enemy_player
		,CAST(COALESCE(gameplay_end.healing_pad_pct, gameplay_end1.healing_pad_pct) AS FLOAT64)                   AS gameplay_healing_pad_pct
		,CAST(COALESCE(gameplay_end.healing_pad, gameplay_end1.healing_pad) AS FLOAT64)                           AS gameplay_healing_pad
		,CAST(COALESCE(gameplay_end.score, gameplay_end1.score) AS FLOAT64)                                       AS gameplay_score
		,CAST(COALESCE(gameplay_end.killed_enemy_player, gameplay_end1.killed_enemy_player) AS FLOAT64)           AS gameplay_killed_enemy_player
		,COALESCE(gameplay_end.deposit_point_amount, gameplay_end1.deposit_point_amount)                          AS gameplay_deposit_point_amount
		,COALESCE(gameplay_end.deposit_point_freq, gameplay_end1.deposit_point_freq)                              AS gameplay_deposit_point_freq
	FROM gameplay_start
	LEFT JOIN gameplay_start1
	ON gameplay_start.session_id = gameplay_start1.session_id AND gameplay_start.user_pseudo_id = gameplay_start1.user_pseudo_id
	LEFT JOIN gameplay_end
	ON gameplay_start.session_id = gameplay_end.session_id AND gameplay_start.user_pseudo_id = gameplay_end.user_pseudo_id
	LEFT JOIN gameplay_end1
	ON gameplay_end.session_id = gameplay_end1.session_id AND gameplay_end.user_pseudo_id = gameplay_end1.user_pseudo_id
	LEFT JOIN login_start
	ON gameplay_start.user_id = login_start.user_id AND gameplay_start.event_timestamp > login_start.current_build_timestamp AND gameplay_start.event_timestamp < COALESCE(login_start.next_build_timestamp, 7258118400000000)
	WHERE gameplay_start.session_id IS NOT NULL 
)

SELECT  *
FROM gameplay_final
