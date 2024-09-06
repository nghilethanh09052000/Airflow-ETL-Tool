{% macro get_max_column_value(table, column) %}

{% set max_value_column_query %}
  SELECT MAX({{ column }}) FROM {{ table }}
{% endset %}

{% set results = run_query(max_value_column_query) %}

{% if execute %}
{# Return the first column #}
{% set max_value = results.columns[0].values()[0] %}
{% else %}
{% set max_value = '' %}
{% endif %}

{{ return(max_value) }}

{% endmacro %}


{% macro get_latest_table_suffix(table) %}

{% set results = get_max_column_value(table=table, column="_TABLE_SUFFIX") %}

{{ return(results) }}

{% endmacro %}

