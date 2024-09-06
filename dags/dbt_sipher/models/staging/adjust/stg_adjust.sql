{{- config(
  materialized='view',
) -}}

{%- set metrics = [
 '_yahoo_creative_id_'
 , '_yahoo_creative_name_'
 , '_yahoo_site_id_'
 , '_vizio_content_id_'
 , '_vizio_placement_type_'
 , '_adid_'
 , '_android_id_'
 , '_android_id_md5_'
 , '_att_status_'
 , '_revenue_usd_'
 , '_revenue_usd_cents_'
 , '_city_'
 , '_revenue_cny_'
 , '_revenue_cny_cents_'
 , '_cost_amount_'
 , '_cost_currency_'
 , '_cost_id_md5_'
 , '_cost_type_'
 , '_country_'
 , '_country_subdivision_'
 , '_deeplink_'
 , '_device_model_'
 , '_device_name_'
 , '_device_type_'
 , '_environment_'
 , '_event_cost_id_'
 , '_event_name_'
 , '_event_'
 , '_external_device_id_md5_'
 , '_fire_adid_'
 , '_lifetime_session_count_'
 , '_google_app_set_id_'
 , '_gps_adid_'
 , '_gps_adid_md5_'
 , '_idfa_'
 , '_idfa_md5_'
 , '_idfa_md5_hex_'
 , '_idfa__android_id_'
 , '_idfa__gps_adid_'
 , '_idfa__gps_adid__fire_adid_'
 , '_idfa_upper_'
 , '_idfv_'
 , '_idfv__google_app_set_id_'
 , '_ip_address_'
 , '_is_imported_'
 , '_is_reattributed_'
 , '_isp_'
 , '_language_'
 , '_last_time_spent_'
 , '_mac_md5_'
 , '_mac_sha1_'
 , '_match_type_'
 , '_mcc_'
 , '_mnc_'
 , '_nonce_'
 , '_oaid_'
 , '_oaid_md5_'
 , '_currency_'
 , '_revenue_float_'
 , '_revenue_'
 , '_os_name_'
 , '_os_version_'
 , '_partner_'
 , '_partner_parameters_'
 , '_postal_code_'
 , '_push_token_'
 , '_random_user_id_'
 , '_referrer_'
 , '_reftag_'
 , '_reftags_'
 , '_region_'
 , '_reporting_cost_'
 , '_reporting_currency_'
 , '_reporting_revenue_'
 , '_rida_'
 , '_sdk_version_'
 , '_session_count_'
 , '_sim_slot_ids_'
 , '_third_party_sharing_disabled_'
 , '_tifa_'
 , '_time_spent_'
 , '_timezone_'
 , '_tracking_enabled_'
 , '_tracking_limited_'
 , '_user_agent_'
 , '_vida_'
 , '_web_uuid_'
 , '_win_adid_'
 , '_win_hwid_'
 , '_win_naid_'
 , '_win_udid_'
 , '_tweet_id_'
 , '_twitter_line_item_id_'
 , '_snapchat_ad_id_'
 , '_sk_timer_expiration_'
 , '_sk_app_id_'
 , '_sk_attribution_signature_'
 , '_sk_campaign_id_'
 , '_sk_coarse_conversion_value_'
 , '_sk_conversion_value_'
 , '_sk_did_win_'
 , '_sk_fidelity_type_'
 , '_sk_network_id_'
 , '_sk_payload_'
 , '_sk_postback_sequence_index_'
 , '_sk_redownload_'
 , '_reporting_revenue_max_'
 , '_reporting_revenue_min_'
 , '_sk_source_app_id_'
 , '_sk_source_domain_'
 , '_sk_source_identifier_'
 , '_sk_transaction_id_'
 , '_sk_ts_'
 , '_sk_version_'
 , '_predicted_value_'
 , '_prediction_label_'
 , '_prediction_period_days_'
 , '_sk_conversion_value_device_'
 , '_sk_conversion_value_device_time_'
 , '_sk_conversion_value_status_'
 , '_sk_conversion_value_time_'
 , '_sk_cv_window_expiration_'
 , '_sk_invalid_signature_'
 , '_sk_registered_at_'
 , '_sk_settings_updated_at_'
 , '_roku_content_id_'
 , '_roku_placement_type_'
 , '_dbm_campaign_type_'
 , '_dbm_creative_id_'
 , '_dbm_exchange_id_'
 , '_dbm_external_customer_id_'
 , '_dbm_insertion_order_id_'
 , '_dbm_line_item_id_'
 , '_dbm_line_item_name_'
 , '_dcm_campaign_type_'
 , '_dcm_creative_id_'
 , '_dcm_external_customer_id_'
 , '_dcm_placement_id_'
 , '_dcm_placement_name_'
 , '_dcm_site_id_'
 , '_gmp_product_type_'
 , '_google_ads_ad_type_'
 , '_google_ads_adgroup_id_'
 , '_google_ads_adgroup_name_'
 , '_google_ads_campaign_id_'
 , '_google_ads_campaign_name_'
 , '_google_ads_campaign_type_'
 , '_google_ads_creative_id_'
 , '_google_ads_engagement_type_'
 , '_google_ads_external_customer_id_'
 , '_gbraid_'
 , '_google_ads_keyword_'
 , '_google_ads_matchtype_'
 , '_google_ads_network_subtype_'
 , '_google_ads_network_type_'
 , '_google_ads_placement_'
 , '_google_ads_video_id_'
 , '_wbraid_'
 , '_gclid_'
 , '_search_term_'
 , '_fb_deeplink_account_id_'
 , '_fb_deeplink_ad_id_'
 , '_fb_deeplink_adgroup_id_'
 , '_fb_deeplink_campaign_id_'
 , '_fb_deeplink_campaign_group_id_'
 , '_fb_install_referrer_'
 , '_fb_install_referrer_account_id_'
 , '_fb_install_referrer_adgroup_id_'
 , '_fb_install_referrer_adgroup_name_'
 , '_fb_install_referrer_ad_id_'
 , '_fb_install_referrer_ad_objective_name_'
 , '_fb_install_referrer_campaign_group_id_'
 , '_meta_install_referrer_campaign_group_name_'
 , '_fb_install_referrer_campaign_id_'
 , '_fb_install_referrer_campaign_name_'
 , '_fb_install_referrer_publisher_platform_'
 , '_meta_install_referrer_'
 , '_meta_install_referrer_account_id_'
 , '_meta_install_referrer_actual_timestamp_'
 , '_meta_install_referrer_adgroup_id_'
 , '_meta_install_referrer_adgroup_name_'
 , '_meta_install_referrer_ad_id_'
 , '_meta_install_referrer_ad_objective_name_'
 , '_meta_install_referrer_campaign_group_id_'
 , '_meta_install_referrer_campaign_id_'
 , '_meta_install_referrer_campaign_name_'
 , '_meta_install_referrer_is_click_'
 , '_meta_install_referrer_publisher_platform_'
 , '_activity_kind_'
 , '_adgroup_name_'
 , '_app_name_'
 , '_app_name_dashboard_'
 , '_app_version_'
 , '_app_version_raw_'
 , '_app_version_short_'
 , '_attribution_updated_at_'
 , '_campaign_name_'
 , '_click_referer_'
 , '_click_time_'
 , '_click_time_hour_'
 , '_connection_type_'
 , '_conversion_duration_'
 , '_cpu_type_'
 , '_created_at_'
 , '_created_at_hour_'
 , '_created_at_milli_'
 , '_creative_name_'
 , '_device_atlas_id_'
 , '_device_manufacturer_'
 , '_engagement_time_'
 , '_engagement_time_hour_'
 , '_hardware_name_'
 , '_impression_based_'
 , '_impression_time_'
 , '_impression_time_hour_'
 , '_install_begin_time_'
 , '_install_finish_time_'
 , '_first_tracker_'
 , '_first_tracker_name_'
 , '_installed_at_'
 , '_installed_at_hour_'
 , '_is_deeplink_click_'
 , '_is_google_play_store_downloaded_'
 , '_is_organic_'
 , '_is_s2s_'
 , '_is_s2s_engagement_based_'
 , '_last_session_time_'
 , '_last_tracker_'
 , '_last_tracker_name_'
 , '_network_name_'
 , '_network_type_'
 , '_outdated_tracker_'
 , '_outdated_tracker_name_'
 , '_proxy_ip_address_'
 , '_random_'
 , '_reattributed_at_'
 , '_reattributed_at_hour_'
 , '_received_at_'
 , '_referral_time_'
 , '_reinstalled_at_'
 , '_rejection_reason_'
 , '_san_engagement_times_'
 , '_app_id_'
 , '_store_'
 , '_tracker_name_'
 , '_tracker_'
 , '_uninstalled_at_'
 , '_attribution_expires_at_'
 , '_attribution_ttl_'
 , '_callback_ttl_'
 , '_click_attribution_window_'
 , '_impression_attribution_window_'
 , '_inactive_user_definition_'
 , '_last_fallback_time_'
 , '_last_fallback_time_hour_'
 , '_platform_'
 , '_probmatching_attribution_window_'
 , '_probmatching_reattribution_attribution_ttl_'
 , '_probmatching_reattribution_attribution_window_'
 , '_probmatching_reattribution_fallback_type_'
 , '_probmatching_reattribution_inactive_user_definition_'
 , '_reattribution_attribution_ttl_'
 , '_reattribution_attribution_window_'
 , '_reattribution_fallback_type_'
 , '_within_callback_ttl_'
 , '_iad_ad_id_'
 , '_iad_conversion_type_'
 , '_iad_country_or_region_'
 , '_iad_keyword_'
 , '_iad_keyword_id_'
 , '_iad_org_id_'
 , '_app_token_'
 , '_label_'
 , '_publisher_parameters_'
 , '_secret_id_'

] -%}

WITH replace_null AS (
  SELECT
    snapshot
    {%- for metric in metrics -%}
      , CASE WHEN {{ metric }} = 'nan' THEN NULL ELSE {{ metric }} END AS {{ metric }}
    {% endfor -%}
  FROM 
    {{ source('raw_adjust', 'raw_adjust_hidden_atlas') }}
)
, stg AS (
  SELECT
    _yahoo_creative_id_                             AS yahoo_creative_id
    , _yahoo_creative_name_                         AS yahoo_creative_name
    , _yahoo_site_id_                               AS yahoo_site_id
    , _vizio_content_id_                            AS vizio_content_id
    , _vizio_placement_type_                        AS vizio_placement_type
    , _adid_                                        AS adid
    , _android_id_                                  AS android_id
    , _android_id_md5_                              AS android_id_md5
    , _att_status_                                  AS att_status
    , CAST(_revenue_usd_       AS FLOAT64)          AS revenue_usd
    , CAST(_revenue_usd_cents_ AS FLOAT64)          AS revenue_usd_cents
    , UPPER(_city_)                                 AS city
    , CAST(_revenue_cny_       AS FLOAT64)          AS revenue_cny
    , CAST(_revenue_cny_cents_ AS FLOAT64)          AS revenue_cny_cents
    , CAST(_cost_amount_       AS FLOAT64)          AS cost_amount
    , _cost_currency_                               AS cost_currency
    , _cost_id_md5_                                 AS cost_id_md5
    , _cost_type_                                   AS cost_type
    , UPPER(_country_)                              AS country
    , _country_subdivision_                         AS country_subdivision
    , _deeplink_                                    AS deeplink
    , _device_model_                                AS device_model
    , _device_name_                                 AS device_name
    , _device_type_                                 AS device_type
    , _environment_                                 AS environment
    , _event_cost_id_                               AS event_cost_id
    , _event_name_                                  AS event_name
    , _event_                                       AS event
    , _external_device_id_md5_                      AS external_device_id_md5
    , _fire_adid_                                   AS fire_adid
    , CAST(_lifetime_session_count_ AS FLOAT64)     AS lifetime_session_count
    , _google_app_set_id_                           AS google_app_set_id
    , _gps_adid_                                    AS gps_adid
    , _gps_adid_md5_                                AS gps_adid_md5
    , _idfa_                                        AS idfa
    , _idfa_md5_                                    AS idfa_md5
    , _idfa_md5_hex_                                AS idfa_md5_hex
    , _idfa__android_id_                            AS idfa__android_id
    , _idfa__gps_adid_                              AS idfa__gps_adid
    , _idfa__gps_adid__fire_adid_                   AS idfa__gps_adid__fire_adid
    , _idfa_upper_                                  AS idfa_upper
    , _idfv_                                        AS idfv
    , _idfv__google_app_set_id_                     AS idfv__google_app_set_id
    , _ip_address_                                  AS ip_address
    , CASE 
        WHEN _is_imported_ IN ('0', '0.0') THEN "False" 
        WHEN _is_imported_ IN ('1', '1.0') THEN "True"
        ELSE _is_imported_
    END AS is_imported
    , CASE 
        WHEN _is_reattributed_ IN ('0', '0.0') THEN "False" 
        WHEN _is_reattributed_ IN ('1', '1.0') THEN "True"
        ELSE _is_reattributed_
    END AS is_reattributed
    , _isp_ AS isp
    , UPPER(_language_)                             AS language
    , FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(CAST(_last_time_spent_ AS FLOAT64) AS INT64))) AS last_time_spent
    , _mac_md5_                                     AS mac_md5
    , _mac_sha1_                                    AS mac_sha1
    , _match_type_                                  AS match_type
    , CAST(_mcc_ AS FLOAT64)                        AS mcc
    , CAST(_mnc_ AS FLOAT64)                        AS mnc
    , _nonce_                                       AS nonce
    , _oaid_                                        AS oaid
    , _oaid_md5_                                    AS oaid_md5
    , _currency_                                    AS currency
    , CAST(_revenue_float_ AS FLOAT64)              AS revenue_float
    , CAST(_revenue_       AS FLOAT64)              AS revenue
    , UPPER(_os_name_)                              AS os_name
    , _os_version_                                  AS os_version
    , _partner_                                     AS partner
    , _partner_parameters_                          AS partner_parameters
    , _postal_code_                                 AS postal_code
    , _push_token_                                  AS push_token
    , _random_user_id_                              AS random_user_id
    , _referrer_                                    AS referrer
    , _reftag_                                      AS reftag
    , _reftags_                                     AS reftags
    , UPPER(_region_)                               AS region
    , CAST(_reporting_cost_    AS FLOAT64)          AS reporting_cost
    , _reporting_currency_     AS reporting_currency
    , CAST(_reporting_revenue_ AS FLOAT64)          AS reporting_revenue
    , _rida_                                        AS rida
    , _sdk_version_                                 AS sdk_version
    , CAST(_session_count_      AS FLOAT64)           AS session_count
    , _sim_slot_ids_                                AS sim_slot_ids
    , CASE 
        WHEN _third_party_sharing_disabled_ IN ('0', '0.0') THEN "False" 
        WHEN _third_party_sharing_disabled_ IN ('1', '1.0') THEN "True"
        ELSE _third_party_sharing_disabled_
    END AS third_party_sharing_disabled
    , _tifa_                                     AS tifa
    , FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(CAST(_time_spent_ AS FLOAT64) AS INT64))) AS time_spent
    , _timezone_                                 AS timezone
    , CASE 
        WHEN _tracking_enabled_ IN ('0', '0.0') THEN "False" 
        WHEN _tracking_enabled_ IN ('1', '1.0') THEN "True"
        ELSE _tracking_enabled_
    END AS tracking_enabled
    , CASE 
        WHEN _tracking_limited_ IN ('0', '0.0') THEN "False" 
        WHEN _tracking_limited_ IN ('1', '1.0') THEN "True"
        ELSE _tracking_limited_
    END AS tracking_limited
    , _user_agent_                                  AS user_agent
    , _vida_                                        AS vida
    , _web_uuid_                                    AS web_uuid
    , _win_adid_                                    AS win_adid
    , _win_hwid_                                    AS win_hwid
    , _win_naid_                                    AS win_naid
    , _win_udid_                                    AS win_udid
    , _tweet_id_                                    AS tweet_id
    , _twitter_line_item_id_                        AS twitter_line_item_id
    , _snapchat_ad_id_                              AS snapchat_ad_id
    , _sk_timer_expiration_                         AS sk_timer_expiration
    , _sk_app_id_                                   AS sk_app_id
    , _sk_attribution_signature_                    AS sk_attribution_signature
    , _sk_campaign_id_                              AS sk_campaign_id
    , _sk_coarse_conversion_value_                  AS sk_coarse_conversion_value
    , CAST(_sk_conversion_value_   AS FLOAT64)      AS sk_conversion_value
    , _sk_did_win_                                  AS sk_did_win
    , _sk_fidelity_type_                            AS sk_fidelity_type
    , _sk_network_id_                               AS sk_network_id
    , _sk_payload_                                  AS sk_payload
    , _sk_postback_sequence_index_                  AS sk_postback_sequence_index
    , _sk_redownload_                               AS sk_redownload
    , CAST(_reporting_revenue_max_ AS FLOAT64)      AS reporting_revenue_max
    , CAST(_reporting_revenue_min_ AS FLOAT64)      AS reporting_revenue_min
    , _sk_source_app_id_                            AS sk_source_app_id
    , _sk_source_domain_                            AS sk_source_domain
    , _sk_source_identifier_                        AS sk_source_identifier
    , _sk_transaction_id_                           AS sk_transaction_id
    , _sk_ts_                                       AS sk_ts
    , _sk_version_                                  AS sk_version
    , CAST(_predicted_value_            AS FLOAT64) AS predicted_value
    , _prediction_label_                            AS prediction_label
    , _prediction_period_days_                      AS prediction_period_days
    , CAST(_sk_conversion_value_device_ AS FLOAT64) AS sk_conversion_value_device
    , _sk_conversion_value_device_time_             AS sk_conversion_value_device_time
    , _sk_conversion_value_status_                  AS sk_conversion_value_status
    , _sk_conversion_value_time_                    AS sk_conversion_value_time
    , _sk_cv_window_expiration_                     AS sk_cv_window_expiration
    , _sk_invalid_signature_                        AS sk_invalid_signature
    , TIMESTAMP_SECONDS(CAST(CAST(_sk_registered_at_       AS FLOAT64) AS INT64)) AS sk_registered_at
    , TIMESTAMP_SECONDS(CAST(CAST(_sk_settings_updated_at_ AS FLOAT64) AS INT64)) AS sk_settings_updated_at
    , _roku_content_id_                             AS roku_content_id
    , _roku_placement_type_                         AS roku_placement_type
    , _dbm_campaign_type_                           AS dbm_campaign_type
    , _dbm_creative_id_                             AS dbm_creative_id
    , _dbm_exchange_id_                             AS dbm_exchange_id
    , _dbm_external_customer_id_                    AS dbm_external_customer_id
    , _dbm_insertion_order_id_                      AS dbm_insertion_order_id
    , _dbm_line_item_id_                            AS dbm_line_item_id
    , _dbm_line_item_name_                          AS dbm_line_item_name
    , _dcm_campaign_type_                           AS dcm_campaign_type
    , _dcm_creative_id_                             AS dcm_creative_id
    , _dcm_external_customer_id_                    AS dcm_external_customer_id
    , _dcm_placement_id_                            AS dcm_placement_id
    , _dcm_placement_name_                          AS dcm_placement_name
    , _dcm_site_id_                                 AS dcm_site_id
    , _gmp_product_type_                            AS gmp_product_type
    , _google_ads_ad_type_                          AS google_ads_ad_type
    , _google_ads_adgroup_id_                       AS google_ads_adgroup_id
    , _google_ads_adgroup_name_                     AS google_ads_adgroup_name
    , _google_ads_campaign_id_                      AS google_ads_campaign_id
    , _google_ads_campaign_name_                    AS google_ads_campaign_name
    , _google_ads_campaign_type_                    AS google_ads_campaign_type
    , _google_ads_creative_id_                      AS google_ads_creative_id
    , _google_ads_engagement_type_                  AS google_ads_engagement_type
    , _google_ads_external_customer_id_             AS google_ads_external_customer_id
    , _gbraid_                                      AS gbraid
    , _google_ads_keyword_                          AS google_ads_keyword
    , _google_ads_matchtype_                        AS google_ads_matchtype
    , _google_ads_network_subtype_                  AS google_ads_network_subtype
    , _google_ads_network_type_                     AS google_ads_network_type
    , _google_ads_placement_                        AS google_ads_placement
    , _google_ads_video_id_                         AS google_ads_video_id
    , _wbraid_                                      AS wbraid
    , _gclid_                                       AS gclid
    , _search_term_                                 AS search_term
    , _fb_deeplink_account_id_                      AS fb_deeplink_account_id
    , _fb_deeplink_ad_id_                           AS fb_deeplink_ad_id
    , _fb_deeplink_adgroup_id_                      AS fb_deeplink_adgroup_id
    , _fb_deeplink_campaign_id_                     AS fb_deeplink_campaign_id
    , _fb_deeplink_campaign_group_id_               AS fb_deeplink_campaign_group_id
    , _fb_install_referrer_                         AS fb_install_referrer
    , _fb_install_referrer_account_id_              AS fb_install_referrer_account_id
    , _fb_install_referrer_adgroup_id_              AS fb_install_referrer_adgroup_id
    , _fb_install_referrer_adgroup_name_            AS fb_install_referrer_adgroup_name
    , _fb_install_referrer_ad_id_                   AS fb_install_referrer_ad_id
    , _fb_install_referrer_ad_objective_name_       AS fb_install_referrer_ad_objective_name
    , _fb_install_referrer_campaign_group_id_       AS fb_install_referrer_campaign_group_id
    , _meta_install_referrer_campaign_group_name_   AS meta_install_referrer_campaign_group_name
    , _fb_install_referrer_campaign_id_             AS fb_install_referrer_campaign_id
    , _fb_install_referrer_campaign_name_           AS fb_install_referrer_campaign_name
    , UPPER(_fb_install_referrer_publisher_platform_) AS fb_install_referrer_publisher_platform
    , _meta_install_referrer_                       AS meta_install_referrer
    , _meta_install_referrer_account_id_            AS meta_install_referrer_account_id
    , _meta_install_referrer_actual_timestamp_      AS meta_install_referrer_actual_timestamp
    , _meta_install_referrer_adgroup_id_            AS meta_install_referrer_adgroup_id
    , _meta_install_referrer_adgroup_name_          AS meta_install_referrer_adgroup_name
    , _meta_install_referrer_ad_id_                 AS meta_install_referrer_ad_id
    , _meta_install_referrer_ad_objective_name_     AS meta_install_referrer_ad_objective_name
    , _meta_install_referrer_campaign_group_id_     AS meta_install_referrer_campaign_group_id
    , _meta_install_referrer_campaign_id_           AS meta_install_referrer_campaign_id
    , _meta_install_referrer_campaign_name_         AS meta_install_referrer_campaign_name
    , _meta_install_referrer_is_click_              AS meta_install_referrer_is_click
    , UPPER(_meta_install_referrer_publisher_platform_ ) AS meta_install_referrer_publisher_platform
    , _activity_kind_                               AS activity_kind
    , _adgroup_name_                                AS adgroup_name
    , _app_name_                                    AS app_name
    , _app_name_dashboard_                          AS app_name_dashboard
    , _app_version_                                 AS app_version
    , _app_version_raw_                             AS app_version_raw
    , _app_version_short_                           AS app_version_short
    , TIMESTAMP_SECONDS(CAST(CAST(_attribution_updated_at_             AS FLOAT64)   AS INT64))  AS attribution_updated_at
    , _campaign_name_                                                  AS campaign_name
    , _click_referer_                                                  AS click_referer
    , TIMESTAMP_SECONDS(CAST(CAST(_click_time_                         AS FLOAT64)   AS INT64))  AS click_time
    , EXTRACT(HOUR FROM TIMESTAMP_SECONDS(CAST(CAST(_click_time_hour_  AS FLOAT64)   AS INT64))) AS click_time_hour
    , _connection_type_                                                AS connection_type
    , FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(CAST(CAST(_conversion_duration_ AS FLOAT64) AS INT64))) AS conversion_duration
    , _cpu_type_                                                       AS cpu_type
    , TIMESTAMP_SECONDS(CAST(CAST(_created_at_                         AS FLOAT64)   AS INT64))  AS created_at
    , EXTRACT(HOUR FROM TIMESTAMP_SECONDS(CAST(CAST(_created_at_hour_  AS FLOAT64)   AS INT64))) AS created_at_hour
    , _creative_name_              AS creative_name
    , _device_atlas_id_            AS device_atlas_id
    , UPPER(_device_manufacturer_) AS device_manufacturer
    , TIMESTAMP_SECONDS(CAST(CAST(_engagement_time_                        AS FLOAT64) AS INT64))  AS engagement_time
    , EXTRACT(HOUR FROM TIMESTAMP_SECONDS(CAST(CAST(_engagement_time_hour_ AS FLOAT64) AS INT64))) AS engagement_time_hour
    , _hardware_name_              AS hardware_name
    , CASE 
        WHEN _impression_based_ IN ('0', '0.0') THEN "False" 
        WHEN _impression_based_ IN ('1', '1.0') THEN "True"
        ELSE _impression_based_
    END AS impression_based
    , TIMESTAMP_SECONDS(CAST(CAST(_impression_time_                        AS FLOAT64) AS INT64))  AS impression_time
    , EXTRACT(HOUR FROM TIMESTAMP_SECONDS(CAST(CAST(_impression_time_hour_ AS FLOAT64) AS INT64))) AS impression_time_hour
    , TIMESTAMP_SECONDS(CAST(CAST(_install_begin_time_                     AS FLOAT64) AS INT64))  AS install_begin_time
    , TIMESTAMP_SECONDS(CAST(CAST(_install_finish_time_                    AS FLOAT64) AS INT64))  AS install_finish_time
    , _first_tracker_              AS first_tracker
    , _first_tracker_name_         AS first_tracker_name
    , TIMESTAMP_SECONDS(CAST(CAST(_installed_at_                           AS FLOAT64) AS INT64))  AS installed_at
    , EXTRACT(HOUR FROM TIMESTAMP_SECONDS(CAST(CAST(_installed_at_hour_    AS FLOAT64) AS INT64))) AS installed_at_hour
    , CASE 
        WHEN _is_deeplink_click_ IN ('0', '0.0') THEN "False" 
        WHEN _is_deeplink_click_ IN ('1', '1.0') THEN "True"
        ELSE _is_deeplink_click_
    END AS is_deeplink_click
    , CASE 
        WHEN _is_google_play_store_downloaded_ IN ('0', '0.0') THEN "False" 
        WHEN _is_google_play_store_downloaded_ IN ('1', '1.0') THEN "True"
        ELSE _is_google_play_store_downloaded_
    END AS is_google_play_store_downloaded
    , CASE 
        WHEN _is_organic_ IN ('0', '0.0') THEN "False" 
        WHEN _is_organic_ IN ('1', '1.0') THEN "True"
        ELSE _is_organic_
    END AS is_organic
    , CASE 
        WHEN _is_s2s_ IN ('0', '0.0') THEN "False" 
        WHEN _is_s2s_ IN ('1', '1.0') THEN "True"
        ELSE _is_s2s_
    END AS is_s2s
    , CASE 
        WHEN _is_s2s_engagement_based_ IN ('0', '0.0') THEN "False" 
        WHEN _is_s2s_engagement_based_ IN ('1', '1.0') THEN "True"
        ELSE _is_s2s_engagement_based_
    END AS is_s2s_engagement_based

    , TIMESTAMP_SECONDS(CAST(CAST(_last_session_time_                      AS FLOAT64) AS INT64))   AS last_session_time
    , _last_tracker_                                AS last_tracker
    , _last_tracker_name_                           AS last_tracker_name
    , _network_name_                                AS network_name
    , _network_type_                                AS network_type
    , _outdated_tracker_                            AS outdated_tracker
    , _outdated_tracker_name_                       AS outdated_tracker_name
    , _proxy_ip_address_                            AS proxy_ip_address
    , _random_                                      AS random
    , TIMESTAMP_SECONDS(CAST(CAST(_reattributed_at_                         AS FLOAT64) AS INT64))  AS reattributed_at
    , EXTRACT(HOUR FROM TIMESTAMP_SECONDS(CAST(CAST(_reattributed_at_hour_  AS FLOAT64) AS INT64))) AS reattributed_at_hour
    , TIMESTAMP_SECONDS(CAST(CAST(_received_at_                             AS FLOAT64) AS INT64))  AS received_at
    , TIMESTAMP_SECONDS(CAST(CAST(_referral_time_                           AS FLOAT64) AS INT64))  AS referral_time
    , TIMESTAMP_SECONDS(CAST(CAST(_reinstalled_at_                          AS FLOAT64) AS INT64))  AS reinstalled_at
    , _rejection_reason_                            AS rejection_reason
    , _san_engagement_times_                        AS san_engagement_times
    , _app_id_                                      AS app_id
    , _store_                                       AS store
    , _tracker_name_                                AS tracker_name
    , _tracker_                                     AS tracker
    , TIMESTAMP_SECONDS(CAST(CAST(_uninstalled_at_                          AS FLOAT64) AS INT64))  AS uninstalled_at
    , TIMESTAMP_SECONDS(CAST(CAST(_attribution_expires_at_                  AS FLOAT64) AS INT64))  AS attribution_expires_at
    , _attribution_ttl_                             AS attribution_ttl
    , _callback_ttl_                                AS callback_ttl
    , CAST(_click_attribution_window_               AS FLOAT64)  AS click_attribution_window
    , CAST(_impression_attribution_window_          AS FLOAT64)  AS impression_attribution_window
    , CAST(_inactive_user_definition_               AS FLOAT64)  AS inactive_user_definition
    , TIMESTAMP_SECONDS(CAST(CAST(_last_fallback_time_                        AS FLOAT64) AS INT64))  AS last_fallback_time
    , EXTRACT(HOUR FROM TIMESTAMP_SECONDS(CAST(CAST(_last_fallback_time_hour_ AS FLOAT64) AS INT64))) AS last_fallback_time_hour
    , _platform_                                                 AS platform
    , CAST(_probmatching_attribution_window_                     AS FLOAT64) AS probmatching_attribution_window
    , CAST(_probmatching_reattribution_attribution_ttl_          AS FLOAT64) AS probmatching_reattribution_attribution_ttl
    , CAST(_probmatching_reattribution_attribution_window_       AS FLOAT64) AS probmatching_reattribution_attribution_window
    , _probmatching_reattribution_fallback_type_                             AS probmatching_reattribution_fallback_type
    , CAST(_probmatching_reattribution_inactive_user_definition_ AS FLOAT64) AS probmatching_reattribution_inactive_user_definition
    , CAST(_reattribution_attribution_ttl_                       AS FLOAT64) AS reattribution_attribution_ttl
    , CAST(_reattribution_attribution_window_                    AS FLOAT64) AS reattribution_attribution_window
    , _reattribution_fallback_type_                                          AS reattribution_fallback_type
    , CAST(_within_callback_ttl_                                 AS FLOAT64) AS within_callback_ttl
    , _iad_ad_id_                                                            AS iad_ad_id
    , _iad_conversion_type_                                                  AS iad_conversion_type
    , _iad_country_or_region_                                                AS iad_country_or_region
    , _iad_keyword_                                                          AS iad_keyword
    , _iad_keyword_id_                                                       AS iad_keyword_id
    , _iad_org_id_                                                           AS iad_org_id
    , _app_token_                                                            AS app_token
    , _label_                                                                AS label
    , _publisher_parameters_                                                 AS publisher_parameters
    , _secret_id_                                                            AS secret_id
    , snapshot

  FROM
    replace_null
)

SELECT * FROM stg