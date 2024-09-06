{{- config(
  materialized='table',
) -}}

SELECT
  customer_id
  , campaign_id
  , ad_group_id
  , video_id
  , ad_group_ad_status
  , ROUND(video_duration_millis / 1000, 2) AS video_duration_seconds
  , video_title
FROM
  {{ source('raw_google_adwords', 'ads_Video') }}   
WHERE
  _DATA_DATE = _LATEST_DATE
