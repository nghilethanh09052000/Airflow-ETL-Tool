{{- config(
    materialized = 'table',
    schema = 'raw_aws_atherlabs',
)-}} 

WITH 
raw_user_wallet AS(    
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
),
user_wallet AS(
    SELECT
        DISTINCT COALESCE(user_id, wallet_user_id) AS user_id,
        LOWER(wallet_address) AS wallet_address,
        email,
        name
    FROM
        raw_user_wallet
),
cognito_raw AS(
    SELECT
        user_id,
        SPLIT(connected_wallets, ',') AS wallets,
        email,
        username,
        user_status
    FROM
        {{ ref('stg_aws__ather_id__raw_cognito') }}
),
cognito_user AS(
    SELECT
        DISTINCT user_id,
        CASE WHEN wallets = 'nan' THEN NULL ELSE LOWER(wallets) END AS wallet_address,
        email,
        username,
        user_status
    FROM
        cognito_raw,
        UNNEST(wallets) wallets
),
union_all AS(
    SELECT
        DISTINCT *
    FROM
        (
            SELECT
                DISTINCT user_id,
                wallet_address,
            FROM
                user_wallet
            UNION ALL
            SELECT
                DISTINCT user_id,
                wallet_address,
            FROM
                cognito_user
        )
    ORDER BY
        user_id
),
user_all AS(
    SELECT
        user_id,
        COALESCE(
            ua.wallet_address,
            uw.wallet_address,
            cu.wallet_address
        ) AS wallet_address,
        ARRAY_AGG(
            DISTINCT COALESCE(uw.email, cu.email) IGNORE NULLS
        ) AS email,
        ARRAY_AGG(DISTINCT name IGNORE NULLS) AS name,
        ARRAY_AGG(DISTINCT username IGNORE NULLS) AS cognito_user_name,
        user_status
    FROM
        union_all AS ua
        LEFT JOIN user_wallet AS uw USING(user_id)
        LEFT JOIN cognito_user AS cu USING(user_id)
    GROUP BY
        1,
        2,
        6
    ORDER BY
        user_id
)
SELECT
    *
FROM
    user_all