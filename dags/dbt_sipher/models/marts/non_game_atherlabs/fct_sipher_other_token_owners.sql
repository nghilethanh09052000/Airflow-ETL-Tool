{{  config(
    materialized='table',
    schema='sipher_presentation',
    )
}}

{% set stg_loyalty_tables = {
    'raw_log_claim_lootbox': ref('stg_aws__loyalty__raw_log_claim_lootbox'),
    'raw_log_open_lootbox': ref('stg_aws__loyalty__raw_log_open_lootbox'),
    'raw_log_spaceship': ref('stg_aws__loyalty__raw_log_spaceship'),
    'raw_log_scrap_spaceship_parts': ref('stg_aws__loyalty__raw_log_scrap_spaceship_parts'),
    'raw_pending_mint': ref('stg_aws__loyalty__raw_pending_mint'),
    'raw_burned': ref('stg_aws__loyalty__raw_burned'),
    'raw_onchain_lootbox': ref('stg_opensearch_onchain__raw_lootbox'),
    'raw_onchain_spaceship': ref('stg_opensearch_onchain__raw_spaceship'),
    'raw_onchain_spaceship_parts': ref('stg_opensearch_onchain__raw_spaceship_parts'),
    'raw_onchain_sculpture': ref('stg_opensearch_onchain__raw_sculpture'),
    }
%}

{% set table_max_dates = [] %}
{% for stg_table in stg_loyalty_tables  %}
{% do table_max_dates.append(get_max_column_value(stg_loyalty_tables[stg_table], 'snapshot_date_tzutc')) %}
{% endfor %}

{% set zip_table_and_max_date = zip(table_max_dates, stg_loyalty_tables) | list %}

WITH
{% for table_max_date, stg_loyalty_table in zip_table_and_max_date %}
  {{ "," if not loop.first else "" }}{{stg_loyalty_table}} AS(
    SELECT
      *
    FROM {{ stg_loyalty_tables[stg_loyalty_table] }}
    WHERE snapshot_date_tzutc = '{{table_max_date}}'
  )
{% endfor %}

, lootbox_claim AS(
    SELECT
      wallet_address,
      SUM(IFNULL(quantity,0)) AS lootbox_claimed_quantity
    FROM raw_log_claim_lootbox
    GROUP BY 1
  )

  , lootbox_open AS(
    SELECT
      wallet_address,
      IFNULL(COUNT(lootbox_id),0) AS lootbox_opened_quantity
    FROM raw_log_open_lootbox
    GROUP BY 1
  )

  , lootbox_mint AS(
    SELECT
      wallet_address,
      SUM(IFNULL(amount,0)) AS lootbox_minted_quantity
    FROM raw_pending_mint AS a
    WHERE type = 'Lootbox' AND status = 'Minted'
    GROUP BY 1
  )

  , lootbox_burn AS(
    SELECT
      wallet_address,
      SUM(IFNULL((amount),0)) AS lootbox_burned_quantity
    FROM raw_burned AS a
    WHERE type = 'Lootbox'
    GROUP BY 1
  )

  , lootbox_onchain AS(
    SELECT 
      wallet_address,
      SUM(value) AS lootbox_onchain_quantity
    FROM raw_onchain_lootbox
    GROUP BY 1
  )


  , spaceship_part_open AS(
    SELECT
      wallet_address,
      IFNULL(COUNT(lootbox_id),0)*5 AS spaceship_part_opened_quantity
    FROM raw_log_open_lootbox
    GROUP BY 1
  )

  , spaceship_part_mint AS(
    SELECT
      wallet_address,
      SUM(IFNULL(amount,0)) AS spaceship_part_minted_quantity
    FROM raw_pending_mint AS a
    WHERE type='SpaceshipPart' AND status = 'Minted'
    GROUP BY 1
  )

  , spaceship_part_burn AS(
    SELECT
      wallet_address,
      SUM(IFNULL(amount,0)) AS spaceship_part_burned_quantity
    FROM raw_burned AS a
    WHERE type = 'SpaceshipPart'
    GROUP BY 1
  )

  , spaceship_part_build_dismantle AS(
    SELECT
      wallet_address,
      IFNULL(COUNT(CASE WHEN action = 'build' THEN part_token_ids END),0)*5 AS spaceship_part_built_quantity,
      IFNULL(COUNT(CASE WHEN action = 'dismatle' THEN part_token_ids END),0)*5 AS spaceship_part_dismantled_quantity
    FROM raw_log_spaceship
    GROUP BY 1
  )

  , spaceship_part_scrap AS(
    SELECT
      wallet_address,
      COUNT(IFNULL(new_spaceship_part_token_id,'0')) AS spaceship_part_scrap_quantity
    FROM raw_log_scrap_spaceship_parts AS a
    GROUP BY 1
  )

  ,spaceship_part_onchain AS(
    SELECT 
      wallet_address,
      SUM(value) AS spaceship_part_onchain_quantity
    FROM raw_onchain_spaceship_parts
    GROUP BY 1
  )

  , spaceship_mint AS(
    SELECT
      wallet_address,
      SUM(IFNULL(amount,0)) AS spaceship_minted_quantity
    FROM raw_pending_mint AS a
    WHERE type='Spaceship' AND status = 'Minted'
    GROUP BY 1
  )

  , spaceship_burn AS(
    SELECT
      wallet_address,
      SUM(IFNULL(amount,0)) AS spaceship_burned_quantity
    FROM raw_burned AS a
    WHERE type = 'Spaceship'
    GROUP BY 1
  )

  , spaceship_build_dismantle AS(
    SELECT
      wallet_address,
      IFNULL(COUNT(CASE WHEN action = 'build' THEN part_token_ids END),0) AS spaceship_built_quantity,
      IFNULL(COUNT(CASE WHEN action = 'dismatle' THEN part_token_ids END),0) AS spaceship_dismantled_quantity
    FROM raw_log_spaceship
    GROUP BY 1
  )

  , spaceship_onchain AS(
    SELECT 
      wallet_address,
      SUM(value) AS spaceship_onchain_quantity
    FROM raw_onchain_spaceship
    GROUP BY 1
  )


  , sculpture_onchain AS(
    SELECT 
      wallet_address,
      SUM(value) AS sculpture_onchain_quantity
    FROM raw_onchain_sculpture
    GROUP BY 1
  )

  , joined_data AS(
    SELECT
      * EXCEPT(wallet_address),
      COALESCE(lootbox_claim.wallet_address,
            lootbox_open.wallet_address,
            lootbox_mint.wallet_address,
            lootbox_onchain.wallet_address,
            lootbox_burn.wallet_address,
            spaceship_burn.wallet_address,
            spaceship_mint.wallet_address,
            spaceship_onchain.wallet_address,
            spaceship_build_dismantle.wallet_address,
            spaceship_part_open.wallet_address,
            spaceship_part_burn.wallet_address,
            spaceship_part_mint.wallet_address,
            spaceship_part_scrap.wallet_address,
            spaceship_part_onchain.wallet_address,
            spaceship_part_build_dismantle.wallet_address,
            sculpture_onchain.wallet_address) AS wallet_address
    FROM lootbox_claim
    FULL OUTER JOIN lootbox_open ON lootbox_open.wallet_address = lootbox_claim.wallet_address
    FULL OUTER JOIN lootbox_mint ON lootbox_mint.wallet_address = lootbox_open.wallet_address
    FULL OUTER JOIN lootbox_onchain ON lootbox_onchain.wallet_address = lootbox_mint.wallet_address
    FULL OUTER JOIN lootbox_burn ON lootbox_burn.wallet_address = lootbox_onchain.wallet_address
    FULL OUTER JOIN spaceship_burn ON spaceship_burn.wallet_address = lootbox_burn.wallet_address
    FULL OUTER JOIN spaceship_mint ON spaceship_mint.wallet_address = spaceship_burn.wallet_address
    FULL OUTER JOIN spaceship_onchain ON spaceship_onchain.wallet_address = spaceship_mint.wallet_address
    FULL OUTER JOIN spaceship_build_dismantle ON spaceship_build_dismantle.wallet_address = spaceship_onchain.wallet_address
    FULL OUTER JOIN spaceship_part_open ON spaceship_part_open.wallet_address = spaceship_build_dismantle.wallet_address
    FULL OUTER JOIN spaceship_part_burn ON spaceship_part_burn.wallet_address = spaceship_part_open.wallet_address
    FULL OUTER JOIN spaceship_part_mint ON spaceship_part_mint.wallet_address = spaceship_part_burn.wallet_address
    FULL OUTER JOIN spaceship_part_scrap ON spaceship_part_scrap.wallet_address = spaceship_part_mint.wallet_address
    FULL OUTER JOIN spaceship_part_onchain ON spaceship_part_onchain.wallet_address = spaceship_part_scrap.wallet_address
    FULL OUTER JOIN spaceship_part_build_dismantle ON spaceship_part_build_dismantle.wallet_address = spaceship_part_onchain.wallet_address
    FULL OUTER JOIN sculpture_onchain ON sculpture_onchain.wallet_address = spaceship_part_build_dismantle.wallet_address
  )
 
  SELECT
    wallet_address,
    SUM(IFNULL((lootbox_claimed_quantity - lootbox_opened_quantity - lootbox_minted_quantity + lootbox_burned_quantity),0)) AS lootbox_quantity,
    SUM(IFNULL((spaceship_built_quantity - spaceship_dismantled_quantity - spaceship_minted_quantity + spaceship_burned_quantity),0)) AS spaceship_quantity,
    SUM(IFNULL((spaceship_part_opened_quantity - spaceship_part_minted_quantity + spaceship_part_burned_quantity + spaceship_part_dismantled_quantity - spaceship_part_built_quantity - spaceship_part_scrap_quantity*2),0)) AS spaceship_part_quantity,
    SUM(IFNULL(lootbox_onchain_quantity,0)) AS lootbox_onchain_quantity,
    SUM(IFNULL(spaceship_onchain_quantity,0)) AS spaceship_onchain_quantity,
    SUM(IFNULL(spaceship_part_onchain_quantity,0)) AS spaceship_part_onchain_quantity,
    SUM(IFNULL(sculpture_onchain_quantity,0)) AS sculpture_onchain_quantity,

  FROM joined_data
  GROUP BY wallet_address