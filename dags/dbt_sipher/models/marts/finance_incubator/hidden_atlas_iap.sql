{{- config(
    materialized='table',
    partition_by={
        "field": "event_date",
        "data_type": "date",
    },
) -}}

WITH a AS (
    SELECT 
        PARSE_DATE('%Y%m%d', event_date)                                  AS event_date
        , {{ get_string_value_from_event_params(key="product_id") }}      AS product_id
        , app_info.id       AS app_id
        , app_info.version  AS app_version
        , {{ get_string_value_from_event_params(key="currency") }}        AS currency_name
        , (({{ get_int_value_from_event_params(key="value") }} )/1000000) AS purchase_amount
        , ROUND(event_value_in_usd,2) AS value_in_usd
        , user_id
        , {{ get_string_value_from_event_params(key="played_day") }}      AS played_day
        , {{ get_string_value_from_event_params(key="level") }}           AS level
        , device.category    AS category
        , UPPER(geo.country) AS country
        , platform
        , _TABLE_SUFFIX      AS __table_suffix 
    FROM `hidden-atlas.analytics_402014087.events_*` 
    WHERE 
        event_name = "in_app_purchase" 
            AND PARSE_DATE('%Y%m%d', SUBSTR(_TABLE_SUFFIX, -8)) BETWEEN PARSE_DATE('%Y%m%d', '20230901') AND CURRENT_DATE()
)
, in_app AS (
    SELECT 
        event_date
        , product_id
        , UPPER(REPLACE(product_id, CONCAT(app_id, ".iap."),'')) AS product_name
        , (CASE 
            WHEN app_id = "hidden.atlas.seek.spot.objects.zoom.maps2d" THEN "Hidden Atlas"
            WHEN app_id = "watermelon.suika2048.fruit.merge.drop"      THEN "Watermelon game: Drop fruits"
            ELSE app_id
        END) AS app_name
        , app_version
        , currency_name
        , purchase_amount
        , value_in_usd
        , user_id
        , played_day
        , level
        , category
        , country
        , platform
    FROM a

)

SELECT * FROM in_app
