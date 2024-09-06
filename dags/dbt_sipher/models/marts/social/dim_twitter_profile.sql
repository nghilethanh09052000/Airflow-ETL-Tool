{# {{- config(
  materialized='incremental',
  unique_key='id',
  merge_update_columns = ['username', 'name', 'is_verified', 'user_defined_location', 'description', 'user_defined_url', 'is_protected', 'pinned_tweet_id'],
) -}}

{%- set date_filter = get_max_column_value(
    table=ref('stg_twitter_profile_stats'),
    column='snapshot_date_tzict'
) -%}


SELECT
  id,
  MAX(username) AS username,
  MAX(name) AS name,
  MAX(description) AS description,
  MAX(is_verified) AS is_verified,
  MAX(is_protected) AS is_protected,
  MAX(user_defined_location) AS user_defined_location,
  MAX(user_defined_url) AS user_defined_url,
  MAX(pinned_tweet_id) AS pinned_tweet_id,
  MAX(profile_created_at) AS profile_created_at
FROM {{ ref('stg_twitter_profile_stats') }}
WHERE snapshot_date_tzict = "{{ date_filter }}"
GROUP BY id #}
