{% macro clean_advertising_id(advertising_id_column="device.advertising_id") -%}
    CASE WHEN {{ advertising_id_column }} IN ('', '00000000-0000-0000-0000-000000000000') THEN NULL ELSE {{ advertising_id_column }} END
{%- endmacro %}

{% macro get_string_value_from_user_properties(key) -%}
    (SELECT up.value.string_value FROM UNNEST(user_properties) AS up WHERE up.key = "{{ key }}")
{%- endmacro %}

{% macro get_int_value_from_user_properties(key) -%}
    (SELECT up.value.int_value FROM UNNEST(user_properties) AS up WHERE up.key = "{{ key }}")
{%- endmacro %}

{% macro get_float_value_from_user_properties(key) -%}
    (SELECT up.value.float_value FROM UNNEST(user_properties) AS up WHERE up.key = "{{ key }}")
{%- endmacro %}

{% macro get_double_value_from_user_properties(key) -%}
    (SELECT up.value.double_value FROM UNNEST(user_properties) AS up WHERE up.key = "{{ key }}")
{%- endmacro %}


{% macro get_string_value_from_event_params(key) -%}
    (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "{{ key }}")
{%- endmacro %}

{% macro get_int_value_from_event_params(key) -%}
    (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "{{ key }}")
{%- endmacro %}

{% macro get_float_value_from_event_params(key) -%}
    (SELECT ep.value.float_value FROM UNNEST(event_params) AS ep WHERE ep.key = "{{ key }}")
{%- endmacro %}

{% macro get_double_value_from_event_params(key) -%}
    (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "{{ key }}")
{%- endmacro %}