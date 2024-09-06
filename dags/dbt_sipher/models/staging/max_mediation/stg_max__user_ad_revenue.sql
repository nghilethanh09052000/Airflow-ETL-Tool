{{- config(
    materialized = 'view',
)-}}

SELECT
  PARSE_TIMESTAMP("%F %T", SUBSTR(Date, 0, 19)) AS timestamp_tzutc
  , Ad_Unit_ID                                  AS ad_unit_id
  , Ad_Unit_Name                                AS ad_unit_name
  , Waterfall                                   AS waterfall
  , Ad_Format                                   AS ad_format
  , Placement                                   AS placement
  , UPPER(Country)                              AS country_code
  , Device_Type                                 AS device_type
  , IDFA                                        AS idfa
  , IDFV                                        AS idfv
  , User_ID                                     AS user_id
  , Network                                     AS network
  , CAST(Revenue AS FLOAT64)                    AS revenue
  , Ad_placement                                AS ad_placement
  , Custom_Data                                 AS custom_data
  , app_id
  , platform
FROM {{ source('raw_max_mediation', 'raw_user_ad_revenue') }}
