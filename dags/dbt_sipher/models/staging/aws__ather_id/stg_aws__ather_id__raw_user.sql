{{- config(
    materialized = 'view',
)-}}

SELECT
    id AS user_id,
    CAST(
        TIMESTAMP_MILLIS(
            CAST(
                (
                    CASE WHEN createdAt = 'nan' THEN NULL ELSE createdAt END
                ) AS INT64
            )
        ) AS TIMESTAMP
    ) AS created_at,
    CAST(
        TIMESTAMP_MILLIS(
            CAST(
                (
                    CASE WHEN updatedAt = 'nan' THEN NULL ELSE updatedAt END
                ) AS INT64
            )
        ) AS TIMESTAMP
    ) AS updated_at,
    avatarImage AS avatar_image,
    bannerImage AS banner_image,
    bio,
    cognitoSub AS cognito_sub,
    email,
    CAST(
        (
            CASE WHEN isBanned = 'nan' THEN NULL ELSE isBanned END
        ) AS BOOLEAN
    ) AS is_banned,
    CAST(
        (
            CASE WHEN isVerified = 'nan' THEN NULL ELSE isVerified END
        ) AS BOOLEAN
    ) AS is_verified,
    name,
    subscribeEmail AS subscribe_email,
    dt AS snapshot_date_tzutc
FROM {{ source('raw_atherid_gcs', 'aws__raw_ather_id_user') }}