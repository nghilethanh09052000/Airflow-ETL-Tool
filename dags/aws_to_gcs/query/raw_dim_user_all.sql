CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.raw_aws_atherlabs.raw_dim_user_all` AS
WITH raw_user_wallet AS(
  SELECT
    id AS user_id,
    userId AS wallet_user_id,
    address AS wallet_address,

    user.cognitoSub AS user_cognitoSub,
    wallet.cognitoSub AS wallet_cognitoSub,

    user.createdAt AS user_createdAt,
    user.updatedAt AS user_updatedAt,
    wallet.createdAt AS wallet_createdAt,
    wallet.updatedAt AS wallet_updatedAt,
    
    subscribeEmail,
    avatarImage,
    email,
    name,
    isVerified,
    isBanned,
    bio,
    bannerImage,

    MAX(COALESCE(user.dt, wallet.dt)) AS partition_date

  FROM `sipher-data-platform.raw_atherid_gcs.gcs_external_raw_ather_id_user` user
  LEFT JOIN `sipher-data-platform.raw_atherid_gcs.gcs_external_raw_ather_id_wallet` wallet 
    ON user.id =  wallet.userId
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  
)

, user_wallet AS(
  SELECT DISTINCT
    COALESCE(user_id, wallet_user_id) AS user_id,
    LOWER(wallet_address) AS wallet_address,
    email,
    name
  FROM raw_user_wallet
)

, cognito_raw AS(
  SELECT
    user_id,
    SPLIT(connected_wallets, ',') AS wallets,
    email,
    username,
    UserStatus
  FROM`sipher-data-platform.raw_atherid_gcs.gcs_external_raw_ather_id_user_cognito` 
)

, cognito_user AS(
  SELECT DISTINCT
    user_id,
    CASE
      WHEN wallets = 'nan' THEN NULL
      ELSE  LOWER(wallets) 
    END AS wallet_address,
    email,
    username,
    UserStatus
  FROM cognito_raw, UNNEST(wallets) wallets
)

, union_all AS(
  SELECT 
    DISTINCT * 
  FROM
  (SELECT DISTINCT
      user_id,
      wallet_address,
    FROM user_wallet
  UNION ALL
    SELECT DISTINCT
      user_id,
      wallet_address,
    FROM cognito_user
    )
    
  )

, user_all AS(  SELECT
    user_id,
    COALESCE(ua.wallet_address,uw.wallet_address,cu.wallet_address) AS wallet_address,
    ARRAY_AGG(DISTINCT COALESCE(uw.email, cu.email) IGNORE NULLS) AS email,
    ARRAY_AGG(DISTINCT name IGNORE NULLS) AS name,
    ARRAY_AGG(DISTINCT username IGNORE NULLS) AS cognito_user_name,
    UserStatus
  FROM union_all AS ua
  LEFT JOIN user_wallet AS uw USING(user_id)
  LEFT JOIN cognito_user AS cu USING(user_id)
  GROUP BY 1,2,6
)

SELECT 
  us.*
  , co.first_event_date
  , co.city
  , co.country
FROM user_all us
LEFT JOIN `sipher-data-platform.dbt_sipher.dim_ather_user__country` co
ON us.user_id = co.user_id