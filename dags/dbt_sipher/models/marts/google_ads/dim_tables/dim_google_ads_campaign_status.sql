{{- config(
  materialized='table',
) -}}

SELECT
  customer_id
  , campaign_id
  , campaign_name
  , campaign_serving_status
  , campaign_status
  , campaign_advertising_channel_sub_type
  , campaign_advertising_channel_type
  , campaign_bidding_strategy_type
  , (campaign_budget_amount_micros / 1000000) AS campaign_budget_amount
  , campaign_budget_explicitly_shared
  , campaign_budget_has_recommended_budget
  , campaign_budget_period
  , campaign_experiment_type
  , campaign_manual_cpc_enhanced_cpc_enabled
FROM
  {{ source('raw_google_adwords', 'ads_Campaign') }}     
WHERE
  _DATA_DATE = _LATEST_DATE
