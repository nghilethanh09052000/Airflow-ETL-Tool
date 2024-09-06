{{- config(
    materialized = 'table',
    partition_by={
    'field': 'day0_date_tzutc',
    'data_type': 'DATE',
  }
)-}}

WITH rev_temp AS (
    SELECT  
        day0_date_tzutc
        , COALESCE(day0_country, UPPER(country_code)) AS day0_country
        , day_diff
        , fiat_currency_price AS gross_revenue
    FROM `sipher-data-platform.sipher_odyssey_shop_transaction.mart_sipher_odyssey_shop_transaction` 
    WHERE fiat_type != 'in game' AND day0_date_tzutc IS NOT NULL
)
, rev AS (
    SELECT
        day0_date_tzutc
        , COALESCE(c.country_code, r.day0_country) AS day0_country_code
        , day_diff
        , SUM(gross_revenue) AS gross_revenue
    FROM rev_temp r
    LEFT JOIN {{ source('dbt_sipher', 'country_codes') }} c
    ON r.day0_country = c.country
    GROUP BY ALL
)
, co AS (
    SELECT
    date
    , geo
    , SUM(cost) AS cost
    FROM {{ source('raw_appsflyer_cost_etl', 'fct_appsflyer_cost_etl_channel') }}
    WHERE app_id != 'id6462842931'
    GROUP BY ALL
)
, installs AS (
    SELECT
        date_tzutc
        , country_code
        , SUM(cnt_installs) AS installs
    FROM {{ ref('in_fin_sipher_installs') }} 
    WHERE media_source != 'organic'
    GROUP BY ALL
)
, t_final AS (
    SELECT 
        COALESCE(rev.day0_date_tzutc, co.date)      AS day0_date_tzutc
        , COALESCE(rev.day0_country_code, co.geo)   AS country_code
        , COALESCE(rev.day_diff, 0)                 AS day_diff
        , COALESCE(rev.gross_revenue, 0)            AS gross_revenue
        , COALESCE(co.cost, 0)                      AS cost
    FROM rev
    FULL JOIN co
    ON rev.day0_date_tzutc = co.date AND rev.day0_country_code = co.geo
)
, final AS (
    SELECT 
        tf.*
        , COALESCE(i.installs, 0) AS installs
    FROM t_final tf
    Full JOIN installs i
    ON tf.day0_date_tzutc = i.date_tzutc AND tf.country_code = i.country_code
)
, a AS (
    SELECT 
        day0_date_tzutc
        , final.country_code AS country_code
        , c.country AS country
        , day_diff
        , installs
        , gross_revenue
        , cost
        , CASE 
            WHEN cost = 0 THEN NULL
            ELSE gross_revenue / cost 
        END AS roas
    FROM final
    LEFT JOIN {{ source('dbt_sipher', 'country_codes') }} c
    ON final.country_code = c.country_code
)
, ret AS (
    SELECT
        day0_date_tzutc
        , day0_country
        , SUM(cohort_size)     AS cohort_size
        , day_diff
        , SUM(retention_count) AS retention_count
    FROM `sipher-data-platform.reporting_sipher_odyssey__game_product.fact_game_product_health__all_view_retention`
    GROUP BY ALL
)

SELECT 
    ver.version
    , a.*
    , ret.cohort_size     AS cohort_size
    , ret.retention_count AS retention_count
FROM a
LEFT JOIN ret 
ON a.day0_date_tzutc = ret.day0_date_tzutc AND a.country = ret.day0_country AND a.day_diff = ret.day_diff
LEFT JOIN {{ ref('dim_fin_sipher_version') }} ver
ON a.day0_date_tzutc BETWEEN ver.from_date AND ver.to_date
