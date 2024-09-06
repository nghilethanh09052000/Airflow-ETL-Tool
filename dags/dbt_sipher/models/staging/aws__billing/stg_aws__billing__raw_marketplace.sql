{{- config(
    materialized = 'view',
)-}}

SELECT
    identity_LineItemId AS identity_line_item_id,
    DATE(bill_BillingPeriodStartDate) AS bill_billing_period_start_date,
    DATE(lineItem_UsageStartDate) AS line_item_usage_start_date,
    lineItem_ProductCode AS line_item_product_code,
    CAST(lineItem_UnblendedCost AS NUMERIC) AS line_item_unblended_cost,
    CAST(lineItem_BlendedCost AS NUMERIC) AS line_item_blended_cost,
    lineItem_LineItemDescription AS line_item_line_item_description,
    lineItem_LineItemType AS line_item_line_item_type,
    product_region AS product_region,
    resourceTags_user_eks_cluster_name AS resource_tag_cluster_name,
    resourceTags_user_project AS resource_tag_project,
    resourceTags_user_type AS resource_tag_type,
    dt AS snapshot_date_tzutc
FROM {{ source('raw_aws_billing_gcs', 'billing__raw_marketplace') }}