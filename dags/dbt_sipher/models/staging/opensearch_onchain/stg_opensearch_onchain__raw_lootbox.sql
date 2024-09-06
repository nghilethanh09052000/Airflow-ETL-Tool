{{- config(
    materialized = 'view',
)-}}

SELECT
    LOWER(owner) AS wallet_address,
    tokenID AS token_id,
    chainId AS chain_id,
    id,
    type,
    collectionId AS collection_id,
    CAST(value AS INT64) AS value,
    dt AS snapshot_date_tzutc
FROM {{ source('raw_aws_opensearch_onchain_nft', 'opensearch_onchain__raw_lootbox') }} AS a