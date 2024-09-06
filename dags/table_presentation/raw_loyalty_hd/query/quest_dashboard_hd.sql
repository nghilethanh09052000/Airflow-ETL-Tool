CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_presentation.quest_dashboard_hd` AS
WITH nomean AS
( SELECT'no' AS nomean
) , results AS
(
	SELECT  a.user_id
	       ,a.user_pseudo_id
	       ,CASE WHEN user_properties.key = 'wallet_address' THEN user_properties.value.string_value END AS wallet_address
	       ,CASE WHEN user_properties.key = 'ather_id' THEN user_properties.value.int_value END           AS ather_id -- user_id = ather_id

	       ,a.traffic_source.source                                                                      AS traffic_source
	       ,TIMESTAMP_seconds( cast(event_timestamp/1000000 AS int64 ))                                  AS event_time
	       ,geo.country
	-- FROM `sipher-atherlabs-ga.analytics_309570267.events_*` a 
	FROM `sipher-atherlabs-ga.analytics_341181237.events_*` a 
	, UNNEST
	(a.user_properties
	) AS user_properties
	WHERE true
	AND event_name = 'ather_club_quest' 
) , event_final AS
(
	SELECT  date(event_time)event_time
	       ,coalesce(ather_id, cast(user_id AS int64)) AS user_id
	       ,wallet_address
	       ,user_pseudo_id
	       ,traffic_source
	       ,country
	FROM results r
) , quest AS
(
	SELECT  distinct date(createdAt)createdAt
	       ,cast(userId                                                                                 AS int64) quest_userId
	       ,questTitle
	       ,ROW_NUMBER( ) OVER (PARTITION BY userId,date(createdAt) ORDER BY timestamp(createdAt) desc) AS updatedAtRank
	       ,concat(userId,questTitle,format_datetime("%F %T",timestamp(createdAt)) )as trans_quest_id
	FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_claim_quest`
) , firstdayquest AS
(
	SELECT  quest_userId
	       ,MIN(createdAt) first_day_quest
	FROM quest
	GROUP BY  1
) , point AS
(
	SELECT  distinct createdAt
	       ,userId                                                                  AS point_userid
	       ,SUM( totalXp ) OVER (PARTITION BY userId ORDER BY createdAt asc)        AS totalXp
	       ,SUM( totalNanochips ) OVER (PARTITION BY userId ORDER BY createdAt asc) AS totalNanochips
	FROM
	(
		SELECT  distinct timestamp(createdAt)createdAt
		       ,cast(userId          AS int64) userId
		       ,(cast(totalXp        AS float64))totalXp
		       ,(cast(totalNanochips AS float64))totalNanochips
		FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_claim_quest`
	)
	ORDER BY createdAt
) , onchain AS
(
	SELECT  distinct cast(id                                                                        AS int64) userId
	       ,date(updatedAt)updatedAt
	       ,sipherNfts
	       ,sipherTokens
	       ,ROW_NUMBER( ) OVER (PARTITION BY id,date(updatedAt) ORDER BY timestamp(updatedAt) desc) AS updatedAtRank
	FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_user_management_info`
)
SELECT  distinct e.*
       ,q.quest_userId
       ,q.questTitle
       ,trans_quest_id
       ,CASE WHEN date_diff(q.createdAt,fq.first_day_quest,day) >= 365 THEN '1year'
             WHEN date_diff(q.createdAt,fq.first_day_quest,day) >= 180 THEN '6months'
             WHEN date_diff(q.createdAt,fq.first_day_quest,day) >= 90 THEN '3months'
             WHEN date_diff(q.createdAt,fq.first_day_quest,day) >= 30 THEN '1month'
             WHEN date_diff(q.createdAt,fq.first_day_quest,day) < 30 THEN 'new' END AS lifetime
       ,p.point_userid
       ,p.totalXp
       ,totalNanochips
       ,`sipher-data-platform.sipher_presentation.xp_to_rank`(p.totalXp)            AS rank_
       ,`sipher-data-platform.sipher_presentation.xp_to_level`(p.totalXp)           AS level
       ,sipherNfts
       ,sipherTokens
FROM event_final e
LEFT JOIN quest q
ON e.user_id = q.quest_userId AND date(event_time) = date(q.createdAt) AND q.updatedAtRank = 1
LEFT JOIN firstdayquest fq
ON q.quest_userId = fq.quest_userId
LEFT JOIN point p
ON e.user_id = p.point_userid AND date(event_time) = date(p.createdAt)
LEFT JOIN onchain o
ON e.user_id = o.userid AND date(event_time) = date(o.updatedAt) AND o.updatedAtRank = 1