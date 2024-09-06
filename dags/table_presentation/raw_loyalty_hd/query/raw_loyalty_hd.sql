CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_presentation.raw_loyalty_hd`
AS

with distribute as (
SELECT distinct a.to as  publicAddressdistribute
--, spaceshipPartTokenIds,atherId,newSpaceshipPartTokenId
, date(updatedAt) as updatedAtdistribute
,CAST(quantity AS INT64)as quantity_distribute
FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.raw_loyalty_log_distribute_lootbox` as a 
)


,claim as (
SELECT distinct publicAddress as publicAddressclaim
--, quantity,tokenid
,date(updatedAt) as updatedAtclaim
FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.raw_loyalty_log_claim_lootbox`
)


,open as (
SELECT distinct publicAddress as publicAddressopen

,date(updatedAt) as updatedAtopen
,count(*) Loot_box_count
FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.raw_loyalty_log_open_lootbox`
group by 1,2
)

,pending_mint as (
SELECT distinct a.to as publicAddresspending_mint
,signature
, status
, type AS mint_type
, date(updatedAt) as updatedAtpending_mint
FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.raw_loyalty_pending_mint` as a
)

,spaceship as (
SELECT distinct publicAddress as
 publicAddressspaceship
 ---, atherId, partTokenIds
 ,	action	,date(updatedAt) as	updatedAtspaceship
FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.raw_loyalty_log_spaceship` as a
)

,burned as (
SELECT distinct a.to AS publicAddress_burned
    , date(updatedAt) as updatedAtburned
    ,SAFE_CAST(amount AS INT64)as quantity_burned
    ,type AS burned_type
FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.raw_loyalty_burned` as a
)

,scrap as (
SELECT distinct publicAddress as publicAddressscrap
---,	atherId	,spaceshipPartTokenIds,		newSpaceshipPartTokenId	
,	date(updatedAt) as 	updatedAtscrap
FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.raw_loyalty_log_scrap_spaceship_parts` as a
)

,atheruser as (
select distinct user_id, wallet_address
from `sipher-data-platform.raw_aws_atherlabs.raw_dim_user_all`
)

select *
from atheruser a 
full JOIN distribute d ON a.wallet_address = d.publicAddressdistribute 

full JOIN claim c ON a.wallet_address = c.publicAddressclaim and c.updatedAtclaim = d.updatedAtdistribute 

full JOIN open o ON a.wallet_address = o.publicAddressopen and o.updatedAtopen = d.updatedAtdistribute 

full JOIN pending_mint p ON a.wallet_address = p.publicAddresspending_mint and p.updatedAtpending_mint = d.updatedAtdistribute 

full JOIN burned b ON a.wallet_address = b.publicAddress_burned and b.updatedAtburned = d.updatedAtdistribute 

full JOIN spaceship s ON a.wallet_address = s.publicAddressspaceship and s.updatedAtspaceship = d.updatedAtdistribute 

full JOIN scrap sc ON a.wallet_address = sc.publicAddressscrap and sc.updatedAtscrap = d.updatedAtdistribute