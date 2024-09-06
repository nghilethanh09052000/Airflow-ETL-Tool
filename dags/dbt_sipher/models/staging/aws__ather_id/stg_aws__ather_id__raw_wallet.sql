{{- config(
    materialized = 'view',
)-}}

SELECT
    userId AS user_id,
    address AS wallet_address,
    cognitoSub AS cognito_sub,
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
    dt AS snapshot_date_tzutc
FROM {{ source('raw_atherid_gcs', 'aws__raw_ather_id_wallet') }}