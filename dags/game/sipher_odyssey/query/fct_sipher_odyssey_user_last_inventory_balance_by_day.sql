CREATE OR REPLACE TABLE `{{ params.bq_project }}.sipher_odyssey_inventory.fct_sipher_odyssey_user_last_inventory_balance_by_day_{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}`
AS

SELECT * FROM `{{ params.bq_project }}.sipher_odyssey_inventory.fct_sipher_odyssey_user_latest_inventory_balance`
WHERE snapshot_date = '{{ ds }}'