{{- config(
    materialized = 'view',
)-}}

SELECT
    user_id,
    Username AS username,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', UserCreateDate) AS user_create_date,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', UserLastModifiedDate) AS user_last_modified_date,
    Attributes AS attributes,
    connected_wallets,
    email,
    email_verified,
    identities,
    CAST(
        (
            CASE WHEN Enabled = 'nan' THEN NULL ELSE Enabled END
        ) AS BOOLEAN
    ) AS is_enabled,
    name,
    sub,
    UserStatus AS user_status,
    providerName AS provider_name,
    providerType AS provider_type,
    issuer,
    primary,
    CAST(dateCreated AS INT64) AS date_created,
    dt AS snapshot_date_tzutc
FROM {{ source('raw_atherid_gcs', 'aws__raw_ather_id_cognito') }}