{{- config(
    materialized = 'table',
    schema = 'raw_aws_atherlabs',
)-}}

SELECT
    user.user_id AS user_id,
    wallet.user_id AS wallet_user_id,
    wallet_address,
    user.cognito_sub AS user_cognito_sub,
    wallet.cognito_sub AS wallet_cognito_sub,
    user.created_at AS user_created_at,
    user.updated_at AS user_updated_at,
    wallet.created_at AS wallet_created_at,
    wallet.updated_at AS wallet_updated_at,
    subscribe_email,
    avatar_image,
    email,
    name,
    is_verified,
    is_banned,
    bio,
    banner_image,
    MAX(COALESCE(user.snapshot_date_tzutc, wallet.snapshot_date_tzutc)) AS partition_date
FROM
    {{ ref('stg_aws__ather_id__raw_user') }} AS user
    LEFT JOIN {{ ref('stg_aws__ather_id__raw_wallet') }} AS wallet ON user.user_id = wallet.user_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
