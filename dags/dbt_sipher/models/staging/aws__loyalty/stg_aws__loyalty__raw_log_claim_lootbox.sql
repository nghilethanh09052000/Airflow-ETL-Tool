{{- config(
    materialized = 'view',
)-}}

SELECT
    id AS user_id,
    publicAddress AS wallet_address,
    SAFE_CAST(quantity AS INT64) AS quantity,
    tokenId AS token_id,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', createdAt) AS created_at,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', updatedAt) AS updated_at,
    CAST(
        (
            CASE WHEN isRandom = 'nan' THEN NULL ELSE isRandom END
        ) AS BOOLEAN
    ) AS is_random,
    dt AS snapshot_date_tzutc
FROM {{ source('raw_loyalty_dashboard_gcs', 'loyalty__raw_log_claim_lootbox') }}
