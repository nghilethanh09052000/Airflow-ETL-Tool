{{- config(
    materialized = 'table',
    partition_by={
		'field': 'date',
		'data_type': 'DATE',
  }
)-}}

WITH raw AS (
  SELECT  
    last_updated_date
    , UPPER(country_code) AS country_code
    , UPPER(
      CASE
        WHEN day0_os IS NULL AND fiat_type ='Google' THEN 'ANDROID'
        WHEN day0_os IS NULL AND fiat_type ='Apple' THEN 'IOS'
        WHEN day0_os IS NULL THEN fiat_type
        ELSE day0_os
      END
      ) AS os
    , fiat_type
    , SUM(fiat_currency_price) AS price
  FROM `sipher-data-platform.sipher_odyssey_shop_transaction.mart_sipher_odyssey_shop_transaction` 
  WHERE fiat_type != 'in game'
  GROUP BY ALL
)
, iap as (
  SELECT * FROM
    (SELECT
      last_updated_date, country_code, os, fiat_type, price
    FROM raw
    WHERE os = 'ANDROID')
  PIVOT (SUM(price) FOR fiat_type IN ('Apple', 'Google', 'Xsolla', 'Crypto'))

  UNION ALL

  SELECT * FROM
    (SELECT
      last_updated_date, country_code, os, fiat_type, price
    FROM raw
    WHERE os = 'IOS')
  PIVOT (SUM(price) FOR fiat_type IN ('Apple', 'Google', 'Xsolla', 'Crypto'))
  
  UNION ALL

  SELECT * FROM
    (SELECT
      last_updated_date, country_code, os, fiat_type, price
    FROM raw
    WHERE os = 'CRYPTO')
  PIVOT (SUM(price) FOR fiat_type IN ('Apple', 'Google', 'Xsolla', 'Crypto'))
  
  UNION ALL
  
  SELECT * FROM
    (SELECT
      last_updated_date, country_code, os, fiat_type, price
    FROM raw
    WHERE os = 'XSOLLA')
  PIVOT (SUM(price) FOR fiat_type IN ('Apple', 'Google', 'Xsolla', 'Crypto'))
)
, co AS (
  SELECT
  date
  , os
  , geo
  , SUM(cost) AS cost
  FROM {{ source('raw_appsflyer_cost_etl', 'fct_appsflyer_cost_etl_channel') }}
  WHERE app_id != 'id6462842931'
  GROUP BY ALL
)
, installs AS (
  SELECT
  date_tzutc
  , os
  , country_code
  , SUM(cnt_installs) AS installs
  FROM {{ ref('in_fin_sipher_installs') }}
  WHERE media_source != 'organic'
  GROUP BY ALL
)
, rev AS (
  SELECT 
    last_updated_date
    , country_code
    , os
    , (COALESCE(Apple,0) + COALESCE(Google,0) + COALESCE(Xsolla,0) + COALESCE(Crypto,0)) AS gross_revenue
    , Apple 
    , Google
    , Xsolla
    , Crypto
  FROM iap
)
, t_final AS (
SELECT 
  COALESCE(rev.last_updated_date, co.date) AS date
  , COALESCE(rev.country_code, co.geo)     AS country_code
  , COALESCE(rev.os, co.os)                AS os
  , COALESCE(rev.gross_revenue, 0)         AS gross_revenue
  , COALESCE(co.cost, 0)    AS cost
  , COALESCE(rev.Apple, 0)  AS rev_apple
  , COALESCE(rev.google, 0) AS rev_google
  , COALESCE(rev.xsolla, 0) AS rev_xsolla
  , COALESCE(rev.crypto, 0) AS rev_crypto
FROM rev
FULL JOIN co
ON rev.last_updated_date = co.date AND rev.country_code = co.geo AND rev.os = co.os

)
, final AS (
  SELECT
    tf.*
    , i.installs AS installs
  FROM t_final tf
  Full JOIN installs i
  ON tf.date = i.date_tzutc AND tf.country_code = i.country_code AND tf.os = i.os

)
  SELECT 
    final.*
    , code.country 
  FROM final 
  LEFT JOIN {{ source('dbt_sipher', 'country_codes') }} code
  ON final.country_code = code.country_code
  WHERE NOT (gross_revenue = 0 AND cost = 0 AND installs = 0)