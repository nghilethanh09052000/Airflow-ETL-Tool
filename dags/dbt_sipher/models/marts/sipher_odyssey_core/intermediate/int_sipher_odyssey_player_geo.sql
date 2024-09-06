{{- config(
  materialized='incremental',
  unique_key='game_user_id'
) -}}


WITH raw AS (
  SELECT
    event_timestamp
    ,user_id
    ,user_pseudo_id
    ,geo.city
    ,geo.country
    ,geo.continent
    ,geo.region
    ,geo.sub_continent
    ,geo.metro
    ,{{ get_string_value_from_user_properties(key="ather_id") }} AS ather_id
  FROM 
    {{ ref('stg_firebase__sipher_odyssey_events_14d') }}
)

,geo_properties AS (
    SELECT
      user_id
      ,ather_id
      ,user_pseudo_id
      ,MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_used_at
      ,MIN(city) AS city
      ,MIN(country) AS country
      ,MIN(continent) AS continent
      ,MIN(region) AS region
      ,MIN(sub_continent) AS sub_continent
      ,MIN(metro) AS metro
    FROM 
      raw
    GROUP BY 
      user_id
      ,ather_id
      ,user_pseudo_id
)

,game_user_ids AS (
  SELECT
    user_id AS game_user_id
    ,CASE WHEN ather_id IS NULL THEN 'NULL' ELSE ather_id END AS ather_id
    ,ARRAY_AGG(DISTINCT user_pseudo_id) AS user_pseudo_ids
  FROM raw
  WHERE 
    user_id IS NOT NULL
  GROUP BY 
    game_user_id
    ,ather_id
)

,joined_geo_to_game_user_id AS (
  SELECT
    game_user_ids.game_user_id 
    ,game_user_ids.ather_id
    ,MIN(first_used_at) AS first_used_at
    ,MIN(city) AS city
    ,MIN(country) AS country
    ,MIN(continent) AS continent
    ,MIN(region) AS region
    ,MIN(sub_continent) AS sub_continent
    ,MIN(metro) AS metro
  FROM  
    game_user_ids
    ,UNNEST(user_pseudo_ids) AS user_pseudo_id
  LEFT JOIN
    geo_properties
  ON  
    geo_properties.user_pseudo_id = user_pseudo_id
  GROUP BY  
    game_user_ids.game_user_id
    ,game_user_ids.ather_id
)

,grouped_ather_ids AS (
  SELECT 
    game_user_id
    ,ARRAY_AGG(DISTINCT ather_id) AS ather_ids
    ,MIN(first_used_at) AS first_used_at
    ,MIN(city) AS city
    ,MIN(country) AS country
    ,MIN(continent) AS continent
    ,MIN(region) AS region
    ,MIN(sub_continent) AS sub_continent
    ,MIN(metro) AS metro
  FROM 
    joined_geo_to_game_user_id
  GROUP BY
    game_user_id
)

,final AS (
  SELECT
    *
    ,ather_ids[OFFSET(ARRAY_LENGTH(ather_ids) - 1)] as latest_ather_id
  FROM 
    grouped_ather_ids
)


{% if is_incremental() -%}

{%-
  set columns = [
    'game_user_id'
    ,'ather_id'
    ,'first_used_at'
    ,'city'
    ,'country'
    ,'continent'
    ,'region'
    ,'sub_continent'
    ,'metro'
    ,'latest_ather_id'
  ]
-%}

,current_ids_and_new_ids AS (

  SELECT 
    {% for column in columns -%}
    {{ column }}
      {%- if not loop.last -%}
      ,
      {% endif %}
    {%- endfor %}
  FROM 
    {{ this }}
    ,UNNEST(ather_ids) AS ather_id
  UNION DISTINCT 
  SELECT 
    {% for column in columns -%}
      {{ column }}
      {%- if not loop.last -%}
        ,
      {% endif %}
    {%- endfor %}
  FROM 
    final
    ,UNNEST(ather_ids) AS ather_id
)

,grouped AS (
  SELECT  
    {%- for column in columns -%}
      {%- if column == 'game_user_id' %}
        {{ column }}
      {%- elif column == 'ather_id' -%}
        ARRAY_AGG(DISTINCT {{ column }} ) AS ather_ids
      {%- elif column != 'latest_ather_id' -%}
        MIN({{ column }}) AS {{ column }}
      {%- endif -%}
      {%- if not loop.last -%}
        ,
      {%- endif %}
    {% endfor -%}
  FROM 
    current_ids_and_new_ids
  GROUP BY
    game_user_id
)

SELECT 
  grouped.*
  ,ather_ids[OFFSET(ARRAY_LENGTH(ather_ids) - 1)] as latest_ather_id
  ,{{ load_metadata(sources=[ref('stg_firebase__sipher_odyssey_events_all_time')]) }} AS load_metadata
FROM grouped


{%- else -%}

SELECT
  *,
  {{ load_metadata(sources=[ref('stg_firebase__sipher_odyssey_events_all_time')]) }} AS load_metadata
FROM final

{%- endif -%}