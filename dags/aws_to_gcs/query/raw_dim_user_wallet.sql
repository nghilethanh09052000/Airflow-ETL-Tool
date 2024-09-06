CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.raw_aws_atherlabs.raw_dim_user_wallet` AS

WITH a AS (
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

SELECT 
  a.*
  , co.first_event_date
  , co.city
  , co.country
FROM a
LEFT JOIN `sipher-data-platform.dbt_sipher.dim_ather_user__country` co
ON a.user_id = co.user_id