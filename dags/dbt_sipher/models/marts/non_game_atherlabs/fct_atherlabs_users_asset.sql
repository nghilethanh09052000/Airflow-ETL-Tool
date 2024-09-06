{{- config(
    materialized='incremental',
    schema='sipher_presentation',
)-}}

WITH 
    dim_user AS
  (SELECT
    *
  FROM {{ref('dim_ather_user__all')}})

  ,data_atherlabs AS 
  (
    SELECT DISTINCT
      user_id AS ather_id,
      LOWER(wallet_address) AS wallet_address,
      email AS email,
    FROM dim_user, UNNEST(email) AS email
  )

    ,data_token AS 
    (
        SELECT 
            *
        FROM {{ ref('stg_bq_public_ethereum__token_transfer_today')}}
        UNION ALL 
        SELECT 
            *
        FROM {{ ref('stg_bq_public_ethereum__token_transfer_last_15d')}}
    )

    ,token_pre_results AS 
    (
        SELECT
            from_address AS address,
            token_address,
            token_name,
            -CAST(sipher_value AS NUMERIC)  AS sipher_value
        FROM data_token
        UNION ALL
        SELECT
            to_address AS address,
            token_address,
            token_name,
            CAST(sipher_value AS NUMERIC)  AS sipher_value
        FROM data_token
    )
  
  ,token_results AS
  (
      SELECT
        LOWER(address) AS wallet_address,
        SUM(CASE
            WHEN LOWER(token_name) = "sipher inu" THEN sipher_value
            ELSE 0
            END) AS INU_TOTAL,
        SUM(CASE
            WHEN LOWER(token_name) = "sipher neko" THEN sipher_value
            ELSE 0
            END) AS NEKO_TOTAL,
        SUM(CASE
            WHEN LOWER(token_name) = "sipher token" THEN sipher_value
            ELSE 0
            END) AS TOKEN_TOTAL
    FROM token_pre_results
    GROUP BY 1 
  )

  , other_token_results AS(
    SELECT *
    FROM {{ref('fct_sipher_other_token_owners')}}
  )

  SELECT
    CURRENT_DATE() AS act_date,
    ather_id,
    email,
    wallet_address,
    INU_TOTAL,
    NEKO_TOTAL,
    (INU_TOTAL + NEKO_TOTAL) AS GENEIS_NFT_TOTAL,
    TOKEN_TOTAL,

    lootbox_quantity,
    spaceship_quantity,
    spaceship_part_quantity,
    lootbox_onchain_quantity,
    spaceship_onchain_quantity,
    spaceship_part_onchain_quantity,
    sculpture_onchain_quantity,
    (lootbox_quantity + spaceship_quantity + spaceship_part_quantity) AS total_other_nft, 
    (lootbox_onchain_quantity + spaceship_onchain_quantity + spaceship_part_onchain_quantity + sculpture_onchain_quantity) AS total_other_onchain_nft, 
    (INU_TOTAL + NEKO_TOTAL + lootbox_quantity + spaceship_quantity + spaceship_part_quantity) AS TOTAL_NFT
  FROM data_atherlabs
  FULL OUTER JOIN token_results USING (wallet_address)
  FULL OUTER JOIN other_token_results USING (wallet_address)
  WHERE ather_id != 'nan'
  ORDER BY ather_id DESC