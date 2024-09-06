{% macro load_metadata(sources=[]) -%}
  ARRAY[STRUCT(
      CURRENT_TIMESTAMP() AS data_load_timestamp,
      "{{ sources | join("|") }}" AS data_sources
    )]
{%- endmacro %}