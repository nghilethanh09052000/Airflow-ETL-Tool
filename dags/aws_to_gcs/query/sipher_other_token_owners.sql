CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.sipher_presentation.Sipher_other_token_owners` AS
WITH
  raw_log_claim_lootbox AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_claim_lootbox`
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_claim_lootbox`)
  )

  , raw_log_open_lootbox AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_open_lootbox`
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_open_lootbox`)
  )

  , raw_log_spaceship AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_spaceship`
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_spaceship`)
  )

  , raw_log_scrap_spaceship_parts AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_scrap_spaceship_parts`
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_log_scrap_spaceship_parts`)
  )

  , raw_pending_mint AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_pending_mint`
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_pending_mint`)
  )

  , raw_burned AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_burned`
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_loyalty_dashboard_gcs.gcs_external_raw_loyalty_burned`)
  )

  , raw_onchain_lootbox AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_Lootbox` 
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_Lootbox`)
  )

  , raw_onchain_spaceship AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_Spaceship` 
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_Spaceship`)
  )

  , raw_onchain_spaceship_parts AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_SpaceshipParts` 
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_SpaceshipParts`)
  )

  , raw_onchain_sculpture AS(
    SELECT
      * 
    FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_Sculpture` 
    WHERE dt IN(
      SELECT  
      MAX(dt)
      FROM `sipher-data-platform.raw_aws_opensearch_onchain_nft.raw_opensearch_onchain_Sculpture`)
  )

  , lootbox_claim AS(
    SELECT
      LOWER(publicAddress) AS wallet_address,
      SUM(IFNULL(CAST(quantity AS INT64),0)) AS lootbox_claimed_quantity
    FROM raw_log_claim_lootbox
    GROUP BY 1
  )

  , lootbox_open AS(
    SELECT
      LOWER(publicAddress) AS wallet_address,
      IFNULL(COUNT(lootboxId),0) AS lootbox_opened_quantity
    FROM raw_log_open_lootbox
    GROUP BY 1
  )

  , lootbox_mint AS(
    SELECT
      a.to AS wallet_address,
      SUM(IFNULL(SAFE_CAST(amount As INT64),0)) AS lootbox_minted_quantity
    FROM raw_pending_mint AS a
    WHERE type = 'Lootbox' AND status = 'Minted'
    GROUP BY 1
  )

  , lootbox_burn AS(
    SELECT
      a.to AS wallet_address,
      SUM(IFNULL((SAFE_CAST(amount As INT64)),0)) AS lootbox_burned_quantity
    FROM raw_burned AS a
    WHERE type = 'Lootbox'
    GROUP BY 1
  )

  , lootbox_onchain AS(
    SELECT 
      owner AS wallet_address,
      SUM(CAST(value AS INT64)) AS lootbox_onchain_quantity
    FROM raw_onchain_lootbox
    GROUP BY 1
  )


  , spaceship_part_open AS(
    SELECT
      LOWER(publicAddress) AS wallet_address,
      IFNULL(COUNT(lootboxId),0)*5 AS spaceship_part_opened_quantity
    FROM raw_log_open_lootbox
    GROUP BY 1
  )

  , spaceship_part_mint AS(
    SELECT
      a.to AS wallet_address,
      SUM(IFNULL(SAFE_CAST(amount As INT64),0)) AS spaceship_part_minted_quantity
    FROM raw_pending_mint AS a
    WHERE type='SpaceshipPart' AND status = 'Minted'
    GROUP BY 1
  )

  , spaceship_part_burn AS(
    SELECT
      a.to AS wallet_address,
      SUM(IFNULL(SAFE_CAST(amount As INT64),0)) AS spaceship_part_burned_quantity
    FROM raw_burned AS a
    WHERE type = 'SpaceshipPart'
    GROUP BY 1
  )

  , spaceship_part_build_dismantle AS(
    SELECT
      publicAddress AS wallet_address,
      IFNULL(COUNT(CASE WHEN action = 'build' THEN partTokenIds END),0)*5 AS spaceship_part_built_quantity,
      IFNULL(COUNT(CASE WHEN action = 'dismatle' THEN partTokenIds END),0)*5 AS spaceship_part_dismantled_quantity
    FROM raw_log_spaceship
    GROUP BY 1
  )

  , spaceship_part_scrap AS(
    SELECT
      publicAddress AS wallet_address,
      COUNT(IFNULL(newSpaceshipPartTokenId,'0')) AS spaceship_part_scrap_quantity
    FROM raw_log_scrap_spaceship_parts AS a
    GROUP BY 1
  )

  ,spaceship_part_onchain AS(
    SELECT 
      owner AS wallet_address,
      SUM(CAST(value AS INT64)) AS spaceship_part_onchain_quantity
    FROM raw_onchain_spaceship_parts
    GROUP BY 1
  )

  , spaceship_mint AS(
    SELECT
      a.to AS wallet_address,
      SUM(IFNULL(SAFE_CAST(amount AS INT64),0)) AS spaceship_minted_quantity
    FROM raw_pending_mint AS a
    WHERE type='Spaceship' AND status = 'Minted'
    GROUP BY 1
  )

  , spaceship_burn AS(
    SELECT
      a.to AS wallet_address,
      SUM(IFNULL(SAFE_CAST(amount As INT64),0)) AS spaceship_burned_quantity
    FROM raw_burned AS a
    WHERE type = 'Spaceship'
    GROUP BY 1
  )

  , spaceship_build_dismantle AS(
    SELECT
      publicAddress AS wallet_address,
      IFNULL(COUNT(CASE WHEN action = 'build' THEN partTokenIds END),0) AS spaceship_built_quantity,
      IFNULL(COUNT(CASE WHEN action = 'dismatle' THEN partTokenIds END),0) AS spaceship_dismantled_quantity
    FROM raw_log_spaceship
    GROUP BY 1
  )

  , spaceship_onchain AS(
    SELECT 
      owner AS wallet_address,
      SUM(CAST(value AS INT64)) AS spaceship_onchain_quantity
    FROM raw_onchain_spaceship
    GROUP BY 1
  )


  , sculpture_onchain AS(
    SELECT 
      owner AS wallet_address,
      SUM(CAST(value AS INT64)) AS sculpture_onchain_quantity
    FROM raw_onchain_sculpture
    GROUP BY 1
  )

  , joined_data AS(
    SELECT * 
    FROM lootbox_claim
    FULL OUTER JOIN lootbox_open USING(wallet_address)
    FULL OUTER JOIN lootbox_mint USING(wallet_address)
    FULL OUTER JOIN lootbox_onchain USING(wallet_address)
    FULL OUTER JOIN lootbox_burn USING(wallet_address)
    FULL OUTER JOIN spaceship_burn USING(wallet_address)
    FULL OUTER JOIN spaceship_mint USING(wallet_address)
    FULL OUTER JOIN spaceship_onchain USING(wallet_address)
    FULL OUTER JOIN spaceship_build_dismantle USING(wallet_address)
    FULL OUTER JOIN spaceship_part_open USING(wallet_address)
    FULL OUTER JOIN spaceship_part_burn USING(wallet_address)
    FULL OUTER JOIN spaceship_part_mint USING(wallet_address)
    FULL OUTER JOIN spaceship_part_scrap USING(wallet_address)
    FULL OUTER JOIN spaceship_part_onchain USING(wallet_address)
    FULL OUTER JOIN spaceship_part_build_dismantle USING(wallet_address)
    FULL OUTER JOIN sculpture_onchain USING(wallet_address)
  )
 
  SELECT
    wallet_address,
    IFNULL((lootbox_claimed_quantity - lootbox_opened_quantity - lootbox_minted_quantity + lootbox_burned_quantity),0) AS lootbox_quantity,
    IFNULL((spaceship_built_quantity - spaceship_dismantled_quantity - spaceship_minted_quantity + spaceship_burned_quantity),0) AS spaceship_quantity,
    IFNULL((spaceship_part_opened_quantity - spaceship_part_minted_quantity + spaceship_part_burned_quantity + spaceship_part_dismantled_quantity - spaceship_part_built_quantity - spaceship_part_scrap_quantity*2),0) AS spaceship_part_quantity,
    IFNULL(lootbox_onchain_quantity,0) AS lootbox_onchain_quantity,
    IFNULL(spaceship_onchain_quantity,0) AS spaceship_onchain_quantity,
    IFNULL(spaceship_part_onchain_quantity,0) AS spaceship_part_onchain_quantity,
    IFNULL(sculpture_onchain_quantity,0) AS sculpture_onchain_quantity,

  FROM joined_data
  ORDER BY 4