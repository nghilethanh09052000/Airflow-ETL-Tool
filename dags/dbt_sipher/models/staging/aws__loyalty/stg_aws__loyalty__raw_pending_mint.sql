{{- config(
    materialized = 'view',
)-}}

SELECT
    id AS user_id,
    LOWER(a.to) AS wallet_address,
    batchID AS batch_id,			
    SAFE_CAST(amount AS INT64) AS amount,			
    salt,			
    deadline,			
    status,			
    type,			
    signature,	
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', createdAt) AS created_at,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', updatedAt) AS updated_at,
    dt AS snapshot_date_tzutc
FROM {{ source('raw_loyalty_dashboard_gcs', 'loyalty__raw_pending_mint') }} AS a