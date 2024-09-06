{{- config(
  materialized='table',
) -}}

SELECT
  customer_id
  , campaign_id
  , ad_group_id
  , ad_group_name
  , ad_group_status
  , ad_group_type
  , campaign_bidding_strategy_type
  , ad_group_ad_rotation_mode
  , (ad_group_cpc_bid_micros / 1000000)              AS ad_group_cpc_bid_micros
  , (ad_group_cpm_bid_micros / 1000000)              AS ad_group_cpm_bid_micros
  , (ad_group_cpv_bid_micros / 1000000)              AS ad_group_cpv_bid_micros
  , ad_group_display_custom_bid_dimension
  , (ad_group_effective_target_cpa_micros / 1000000) AS ad_group_effective_target_cpa_micros
  , ad_group_effective_target_cpa_source 
  , ad_group_effective_target_roas
  , ad_group_effective_target_roas_source
FROM
  {{ source('raw_google_adwords', 'ads_AdGroup') }}   
WHERE
  _DATA_DATE = _LATEST_DATE

  