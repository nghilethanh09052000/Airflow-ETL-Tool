{{- config(
    materialized='table',
    schema='sipher_presentation',
)-}}

{% set stg_billing_tables = {
    'blockchain': ref('stg_aws__billing__raw_blockchain'),
    'g1': ref('stg_aws__billing__raw_g1'),
    'marketplace': ref('stg_aws__billing__raw_marketplace'),
    'game_production': ref('stg_aws__billing__raw_game_production'),
    }
%}

WITH
{% for billing_table in stg_billing_tables %}
  {{ "," if not loop.first else "" }}extracted_date_month_{{billing_table}} AS(
    SELECT
      DISTINCT MAX(snapshot_date_tzutc) AS max_dt,
      EXTRACT(MONTH FROM snapshot_date_tzutc) AS act_month
    FROM {{ stg_billing_tables[billing_table] }}
    GROUP BY 2
  )

  ,{{billing_table}} AS
  (SELECT
    bill_billing_period_start_date,
    line_item_usage_start_date,
    line_item_product_code,
    SUM(CAST(line_item_unblended_cost AS NUMERIC)) AS line_item_unblended_cost,
    SUM(CAST(line_item_blended_cost AS NUMERIC)) AS line_item_blended_cost,
    line_item_line_item_description,
    '{{billing_table}}' AS aws_account,
    MAX(snapshot_date_tzutc) AS partition_date
  FROM {{ stg_billing_tables[billing_table] }}
  WHERE line_item_line_item_type = 'Usage'
    AND snapshot_date_tzutc IN (SELECT max_dt FROM extracted_date_month_{{billing_table}})
  GROUP BY 1,2,3,6
  ORDER BY 2)

{% endfor %}

  SELECT * FROM marketplace
  UNION ALL
  SELECT * FROM g1
  UNION ALL
  SELECT * FROM blockchain