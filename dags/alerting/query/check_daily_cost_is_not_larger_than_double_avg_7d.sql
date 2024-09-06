DECLARE multiplier INT64 DEFAULT 2;

WITH
  total_cost AS
    (SELECT
      billing_account_id,
      service,
      sku,
      usage_start_time,
      usage_end_time,
      project,
      labels,
      system_labels,
      location,
      STRUCT(
          CAST(NULL AS STRING) AS name, 
          CAST(NULL AS STRING) AS global_name
      ) AS resource,
      tags,
      STRUCT(
          CAST(NULL AS INT64) AS effective_price,
          CAST(NULL AS INT64) AS tier_start_amount,
          CAST(NULL AS STRING) AS unit,
          CAST(NULL AS INT64) AS pricing_unit_quantity
      ) AS price,
      export_time,
      cost,
      currency,
      currency_conversion_rate,
      usage,
      credits,
      invoice,
      cost_type,
      adjustment_info,
    FROM `sipher-data-platform.gcp_billing.gcp_billing_export_v1_01BB18_596C91_9668DA`
    UNION ALL
    SELECT
      billing_account_id,
      service,
      sku,
      usage_start_time,
      usage_end_time,
      project,
      labels,
      system_labels,
      location,
      resource,
      tags,
      price,
      export_time,
      cost,
      currency,
      currency_conversion_rate,
      usage,
      credits,
      invoice,
      cost_type,
      adjustment_info,
    FROM `data-platform-387707.gcp_billing.gcp_billing_export_resource_v1_0173C9_761D5E_56A3FC`)

  ,data AS
    (SELECT
      DATE(usage_end_time) AS date,
      SUM(cost / currency_conversion_rate) AS cost
    FROM total_cost
    WHERE DATE(usage_end_time) BETWEEN DATE_SUB("{{ ds }}", INTERVAL 14 DAY) AND "{{ ds }}"
    GROUP BY date)

  ,add_bound AS
    (SELECT
      date,
      cost,
      SUM(cost) OVER(ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) / 7 * multiplier AS bound
    FROM data)

SELECT
  *
FROM add_bound
WHERE date = "{{ ds }}"
  AND cost > bound
ORDER BY date
