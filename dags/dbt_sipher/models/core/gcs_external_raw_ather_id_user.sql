{{ config(materialized='table') }}

SELECT DISTINCT 
    user_id AS id 
FROM {{ ref('stg_aws__ather_id__raw_user') }}