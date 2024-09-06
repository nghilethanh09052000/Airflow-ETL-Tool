{{- config(
  materialized='table',
  partition_by={
    'field':'day0_date_tzutc',
    'data_type':'DATE',
  },
  cluster_by=['user_id']
) -}}


WITH
  cohort_date AS 
  (
    SELECT
      DISTINCT
      user_id,
      user_pseudo_id,
      MIN(event_timestamp) AS first_open_timestamp,
      EXTRACT(DATE FROM TIMESTAMP_MICROS (MIN(event_timestamp) )) AS day0_date_tzutc
    FROM
      {{ ref('stg_firebase__sipher_odyssey_events_all_time') }}
    WHERE event_name IN ('login_start')
    AND __table_suffix >= '20240301'
    GROUP BY 1,2
  )

  ,data_login AS (
    SELECT
      DISTINCT
      {{ get_string_value_from_user_properties(key="ather_id") }} AS ather_id,
      {{ get_string_value_from_event_params(key="build_number") }} AS build_number,
      user_id,
      user_pseudo_id,

    FROM {{ ref('stg_firebase__sipher_odyssey_events_all_time') }}
    WHERE event_name IN ('login_start')
    AND __table_suffix >= '20240301'
  )

  ,user_email AS
  (
    SELECT
      *
    FROM {{ ref('stg_aws__ather_id__raw_cognito') }}

  )

  ,email_last_updated AS 
  (
    SELECT
      user_id AS ather_id,
      MAX(user_last_modified_date) AS timestamp_date
    FROM user_email
    GROUP BY 1
  )

  ,email_unique AS 
  (
    SELECT
      DISTINCT
      elu.*
      ,email
      ,ue.name AS user_name
    FROM email_last_updated AS elu
    LEFT JOIN user_email AS ue 
        ON elu.ather_id = ue.user_id
        AND elu.timestamp_date = ue.user_last_modified_date
  )

    ,join_data AS 
    (
    SELECT
      cd.first_open_timestamp,
      cd.day0_date_tzutc,
      lg.*,
      SUBSTR(lg.user_id, 1, 3) AS prefix_user_id,
      SUBSTR(lg.user_id, 4) AS suffix_user_id,
      du.user_name AS user_name,
      du.email AS email
    FROM  data_login AS lg
    LEFT JOIN email_unique AS du ON lg.ather_id = du.ather_id
    LEFT JOIN cohort_date AS cd ON lg.user_pseudo_id = cd.user_pseudo_id AND lg.user_id = cd.user_id
  )

    ,pre_final AS(
    SELECT
        user_id,
        ather_id,
        MIN(day0_date_tzutc) AS day0_date_tzutc,
        email,
        SPLIT(email, '@')[OFFSET(0)] AS prefix_email,
        SPLIT(email, '@')[OFFSET(1)] AS suffix_email,
        user_name,
        prefix_user_id,
        CAST(suffix_user_id AS INT64) AS suffix_user_id,
        MIN(build_number) AS first_build_number,
        MAX(build_number) AS last_build_number,
        MIN(first_open_timestamp) AS first_open_timestamp,
    FROM join_data
    WHERE
        user_id IS NOT NULL
    GROUP BY user_id, ather_id, email, prefix_user_id,suffix_user_id, user_name
    )

    , login_start_raw AS(
    SELECT DISTINCT 
      user_id,
      event_name,
      {{ get_string_value_from_event_params(key='build_number') }} AS build_number,
      app_info.version AS app_version,
      geo.country AS country,
      MIN (event_timestamp) AS current_build_timestamp
    FROM {{ ref('stg_firebase__sipher_odyssey_events_all_time') }}
    WHERE event_name = 'login_start'
    AND __table_suffix >= '20240301'
    GROUP BY user_id, event_name, build_number, app_version, country
    )

    , login_start AS
    (
    SELECT DISTINCT 
      login_start_raw.user_id
      ,ather_id
      ,cohort.day0_date_tzutc AS day0_date_tzutc
      ,build_number
      ,app_version
      ,email
      ,user_name
      ,current_build_timestamp
      ,LEAD(current_build_timestamp,1) OVER (PARTITION BY login_start_raw.user_id ORDER BY current_build_timestamp) AS next_build_timestamp
      ,CASE
        WHEN email LIKE '%atherlabs%' THEN TRUE
        ELSE FALSE
      END AS is_atherlabs_employee
    FROM login_start_raw
    JOIN pre_final cohort
      ON (login_start_raw.user_id = cohort.user_id)
  )

SELECT DISTINCT 
  ls.*,
FROM login_start ls 



   



