{{- config(
    materialized = 'view',
)-}}

SELECT
    id AS user_id,
    LOWER(publicAddress) AS wallet_address,
    atherId AS ather_id,
    spaceshipPartTokenIds AS spaceship_part_token_id,
    caseTypeNumbers AS case_type_numbers,
    newSpaceshipPartTokenId AS new_spaceship_part_token_id,
    newSpaceshipPartCaseTypeNumber AS new_spaceship_part_case_type_number,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', createdAt) AS created_at,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', updatedAt) AS updated_at,
    dt AS snapshot_date_tzutc
FROM {{ source('raw_loyalty_dashboard_gcs', 'loyalty__raw_log_scrap_spaceship_parts') }}