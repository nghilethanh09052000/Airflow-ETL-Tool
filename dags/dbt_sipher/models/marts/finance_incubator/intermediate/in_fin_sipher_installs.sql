{{- config(
    materialized = 'table',
    partition_by={
        'field': 'date_tzutc',
        'data_type': 'DATE',
  }
)-}}

SELECT  
  PARSE_DATE('%Y-%m-%d', dt) AS date_tzutc
  , app_id
  , app_version
  , UPPER(platform) AS os
  , media_source
  , country_code
  , COUNT(event_name) AS cnt_installs
  , COUNT(DISTINCT customer_user_id) AS cnt_users
FROM {{ source('raw_game_appsflyer_locker', 'installs') }}
WHERE app_id = 'com.atherlabs.sipherodyssey.arpg'
GROUP BY ALL

UNION ALL

SELECT  
  PARSE_DATE('%Y-%m-%d', dt) AS date_tzutc
  , app_id
  , app_version
  , UPPER(platform) AS os
  , media_source
  , country_code
  , COUNT(event_name) AS cnt_installs
  , COUNT(DISTINCT customer_user_id) AS cnt_users
FROM {{ source('raw_appsflyer_locker', 'installs') }}
WHERE app_id = 'id6443806584' OR app_id = 'com.atherlabs.sipherodyssey.arpg'
GROUP BY ALL